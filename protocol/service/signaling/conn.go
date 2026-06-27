package signaling

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Happy2018new/nemc-tan-lobby-solver/core/nethernet"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Conn 是到网易 signaling 服务器的 WebSocket 连接,支持底层 ws 断开后自动重连。
//
// 生命周期分两层:
//   - 外层 ctx(c.ctx):房间级。仅 Close()(主动关闭或重连放弃)才取消;Notify 监听它。
//     它存活即表示房间存活,重连对 nethernet.Listener 完全透明。
//   - 会话 sctx(sessionCtx):单条 ws。read/ping 绑定它;ws 出错只取消会话,
//     由 supervise() 重连,不影响外层。
type Conn struct {
	dialer Dialer

	// 拨号参数(重连复用同一 seed/ticket)
	serverBaseAddress string
	g79UserUID        uint32
	signalingSeed     []byte
	signalingTicket   []byte

	// 外层(房间)生命周期
	ctx    context.Context
	cancel context.CancelCauseFunc

	// 当前 ws 会话(sessionMu 保护)
	sessionMu     sync.Mutex
	ws            *websocket.Conn
	sessionCtx    context.Context
	sessionCancel context.CancelCauseFunc

	// 凭据寄存器:buffered(1) 当作"最新值"寄存器;credMu 保证 drain+put 原子,
	// 使重连后新凭据能覆盖旧值,且与 Credentials 的 read-then-putback 互斥。
	credMu      sync.Mutex
	credentials chan nethernet.Credentials
	signals     chan *nethernet.Signal

	log       *slog.Logger
	closeOnce sync.Once
}

// newConn 构造一个尚未连接的 Conn(拨号由 openSession 完成)。
func newConn(dialer Dialer, serverBaseAddress string, g79UserUID uint32, signalingSeed, signalingTicket []byte) *Conn {
	c := &Conn{
		dialer:            dialer,
		serverBaseAddress: serverBaseAddress,
		g79UserUID:        g79UserUID,
		signalingSeed:     signalingSeed,
		signalingTicket:   signalingTicket,
		credentials:       make(chan nethernet.Credentials, 1),
		signals:           make(chan *nethernet.Signal),
		log:               slog.Default().With(slog.String("src", "signaling"), slog.String("room", dialer.LogName)),
	}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	return c
}

// openSession 建立一条新的 ws 会话:拨号 -> 启动 read/ping -> 等待首批凭据。
// ctx 用于取消本次建立过程(首连传调用方 ctx,重连传 c.ctx)。
func (c *Conn) openSession(ctx context.Context) error {
	ws, err := c.dialWebsocket(ctx)
	if err != nil {
		return fmt.Errorf("openSession: %v", err)
	}

	sctx, scancel := context.WithCancelCause(c.ctx)
	c.sessionMu.Lock()
	c.ws = ws
	c.sessionCtx = sctx
	c.sessionCancel = scancel
	c.sessionMu.Unlock()

	firstCred := make(chan struct{})
	go c.read(sctx, ws, firstCred)
	go c.ping(sctx, ws)

	select {
	case <-firstCred:
		return nil
	case <-sctx.Done():
		return fmt.Errorf("openSession: %v", context.Cause(sctx))
	case <-ctx.Done():
		c.endSession(sctx, fmt.Errorf("openSession: %v", ctx.Err()))
		return fmt.Errorf("openSession: %v", ctx.Err())
	case <-time.After(time.Second * 30):
		err := fmt.Errorf("openSession: wait credentials timeout")
		c.endSession(sctx, err)
		return err
	}
}

// endSession 结束指定会话(取消 sctx + 关 ws),交由 supervise 重连。只影响"当前"会话:
// 若 sctx 已不是当前会话(重连已发生),则忽略,避免旧会话的错误误杀新会话。
func (c *Conn) endSession(sctx context.Context, cause error) {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	if c.sessionCtx != sctx {
		return
	}
	if c.sessionCancel != nil {
		c.sessionCancel(cause)
	}
	if c.ws != nil {
		_ = c.ws.Close(websocket.StatusNormalClosure, "")
	}
}

// read 读取并分发当前会话的消息。出错只结束本会话(不拆房)。
func (c *Conn) read(sctx context.Context, ws *websocket.Conn, firstCred chan struct{}) {
	gotFirst := false
	for {
		var message Message
		if err := wsjson.Read(sctx, ws, &message); err != nil {
			c.endSession(sctx, fmt.Errorf("read: %v", err))
			return
		}

		switch message.From {
		case "signalingServer":
			var credentials nethernet.Credentials
			if err := json.Unmarshal([]byte(message.Data), &credentials); err != nil {
				c.endSession(sctx, fmt.Errorf("read: %v", err))
				return
			}
			c.setCredentials(credentials)
			if !gotFirst {
				gotFirst = true
				close(firstCred)
			}
		default:
			var signal nethernet.Signal
			if err := signal.UnmarshalText([]byte(message.Data)); err != nil {
				c.endSession(sctx, fmt.Errorf("read: %v", err))
				return
			}
			signal.NetworkID = message.From
			select {
			case c.signals <- &signal:
			case <-sctx.Done():
				return
			case <-c.ctx.Done():
				return
			}
		}
	}
}

// setCredentials 用最新凭据覆盖寄存器(非阻塞 drain 再 put)。
func (c *Conn) setCredentials(credentials nethernet.Credentials) {
	c.credMu.Lock()
	defer c.credMu.Unlock()
	select {
	case <-c.credentials:
	default:
	}
	c.credentials <- credentials
}

// ping 周期性发送 ping 保活。写失败只结束本会话。
func (c *Conn) ping(sctx context.Context, ws *websocket.Conn) {
	ticker := time.NewTicker(time.Second * 50)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := wsjson.Write(sctx, ws, Message{Type: MessageTypeClientRequestPing}); err != nil {
				c.endSession(sctx, fmt.Errorf("ping: %v", err))
				return
			}
		case <-sctx.Done():
			return
		}
	}
}

// refreshCredentials 向服务器请求刷新凭据;响应经 read -> setCredentials 自动更新寄存器,
// 无需阻塞等待。写失败返回错误(由 autoRefresh 触发重连)。
func (c *Conn) refreshCredentials() error {
	c.sessionMu.Lock()
	ws, sctx := c.ws, c.sessionCtx
	c.sessionMu.Unlock()
	if ws == nil || sctx == nil {
		return fmt.Errorf("refreshCredentials: no active session")
	}
	if err := wsjson.Write(sctx, ws, Message{Type: MessageTypeClientRequestCredentials}); err != nil {
		return fmt.Errorf("refreshCredentials: %v", err)
	}
	return nil
}

// autoRefresh 周期性刷新凭据。绑定外层 ctx;刷新失败不退出,而是触发 ws 重连后继续。
func (c *Conn) autoRefresh(refreshTime time.Duration) {
	if refreshTime == RefreshTimeDisbale {
		return
	}

	ticker := time.NewTicker(refreshTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refreshCredentials(); err != nil {
				c.log.Warn("凭据刷新失败, 触发 ws 重连", slog.Any("error", err))
				c.sessionMu.Lock()
				sctx := c.sessionCtx
				c.sessionMu.Unlock()
				if sctx != nil {
					c.endSession(sctx, err)
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// supervise 监管 ws 会话:当前会话结束后按"连续失败次数"成倍退避自动重连;重连成功清零,
// 连续失败达到上限则放弃并 Close(从而拆除房间,交由上层整房重建)。
func (c *Conn) supervise() {
	policy := c.dialer.Reconnect
	maxFailures := policy.maxFailuresOrDefault()

	for {
		c.sessionMu.Lock()
		sctx := c.sessionCtx
		c.sessionMu.Unlock()

		select {
		case <-sctx.Done():
		case <-c.ctx.Done():
			return
		}
		if c.ctx.Err() != nil {
			return
		}

		if !policy.enabled() {
			c.log.Warn("signaling ws 断开, 未启用自动重连, 关闭", slog.Any("error", context.Cause(sctx)))
			c.Close(fmt.Errorf("supervise: ws closed, reconnect disabled"))
			return
		}

		c.log.Warn("signaling ws 断开, 开始重连", slog.Any("error", context.Cause(sctx)))

		fail := 0
		for {
			d := backoffDelay(policy.Delay, fail+1)
			c.log.Warn(fmt.Sprintf("signaling ws 将在 %d 秒后重连(第 %d 次)", int(d.Seconds()), fail+1))
			select {
			case <-time.After(d):
			case <-c.ctx.Done():
				return
			}

			if err := c.openSession(c.ctx); err == nil {
				c.log.Info("signaling ws 重连成功")
				break // 回到外层循环,fail 自然清零
			} else {
				fail++
				c.log.Warn("signaling ws 重连失败", slog.Int("fail", fail), slog.Any("error", err))
				if fail >= maxFailures {
					c.log.Error("signaling ws 重连达到上限, 放弃(将触发整房重建)", slog.Int("max", maxFailures))
					c.Close(fmt.Errorf("supervise: ws reconnect gave up after %d failures", fail))
					return
				}
			}
		}
	}
}

// Signal sends a Signal to a remote network referenced by [Signal.NetworkID].
func (c *Conn) Signal(signal *nethernet.Signal) error {
	c.sessionMu.Lock()
	ws, sctx := c.ws, c.sessionCtx
	c.sessionMu.Unlock()
	if ws == nil || sctx == nil {
		return fmt.Errorf("Signal: no active session")
	}

	err := wsjson.Write(sctx, ws, Message{
		Type: MessageTypeClientSendSignal,
		To:   json.Number(signal.NetworkID),
		Data: signal.String(),
	})
	if err != nil {
		c.endSession(sctx, fmt.Errorf("Signal: %v", err))
		return fmt.Errorf("Signal: %v", err)
	}
	return nil
}

// Notify registers a Notifier to receive notifications for signals and errors. It returns
// a function to stop receiving notifications on Notifier. Once the stopping function is called,
// ErrSignalingStopped will be notified to the Notifier, and the underlying negotiator should
// handle the error by closing or returning.
func (c *Conn) Notify(n nethernet.Notifier) (stop func()) {
	go func() {
		for {
			select {
			case signal := <-c.signals:
				n.NotifySignal(signal)
			case <-c.ctx.Done():
				n.NotifyError(nethernet.ErrSignalingStopped)
				return
			}
		}
	}()
	return func() {
		c.Close(fmt.Errorf("Notify: Use of closed network connection"))
	}
}

// Credentials blocks until Credentials are received by Signaling, and returns them. If Signaling
// does not support returning Credentials, it will return nil. Credentials are typically received
// from a WebSocket connection. The [context.Context] may be used to cancel the blocking.
func (c *Conn) Credentials(ctx context.Context) (*nethernet.Credentials, error) {
	c.credMu.Lock()
	defer c.credMu.Unlock()

	select {
	case credentials := <-c.credentials:
		c.credentials <- credentials
		return &credentials, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("Credentials: %v", ctx.Err())
	case <-c.ctx.Done():
		return nil, fmt.Errorf("Credentials: %v", c.ctx.Err())
	}
}

// NetworkID returns the local network ID of Signaling. It is used by Listener to obtain its local
// network ID.
func (c *Conn) NetworkID() string {
	return c.dialer.NetherNetID
}

// PongData ..
func (c *Conn) PongData(d []byte) {}

// Close 主动关闭整条连接(取消外层 ctx),会触发 Notify 上报 ErrSignalingStopped 进而拆房。
func (c *Conn) Close(err error) {
	c.closeOnce.Do(func() {
		c.sessionMu.Lock()
		if c.ws != nil {
			_ = c.ws.Close(websocket.StatusNormalClosure, "")
		}
		if c.sessionCancel != nil {
			c.sessionCancel(fmt.Errorf("Close: %v", err))
		}
		c.sessionMu.Unlock()
		c.cancel(fmt.Errorf("Close: %v", err))
	})
}
