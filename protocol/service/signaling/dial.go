package signaling

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/Happy2018new/nemc-tan-lobby-solver/bunker"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

const (
	RefreshTimeDisbale = time.Duration(0)
	RefreshTimeDefault = time.Minute * 30
)

// 重连退避相关常量(成倍增长、封顶、默认失败上限)。
const (
	reconnectMultiplier = 2
	maxReconnectDelay   = 60 * time.Second
	defaultMaxFailures  = 10
)

// ReconnectPolicy 控制 ws 断开后的自动重连行为。退避按"连续失败次数"成倍增长,
// 重连成功即清零;连续失败达到上限则放弃并 Close(交由上层整房重建)。
type ReconnectPolicy struct {
	// Delay 基础重连延迟,按连续失败次数成倍增长(封顶 maxReconnectDelay)。
	// <=0 表示禁用 ws 自动重连(掉线即关闭)。
	Delay time.Duration
	// MaxFailures 连续重连失败达到该数则放弃(Close)。<=0 用默认值 defaultMaxFailures。
	MaxFailures int
}

// enabled 报告是否启用 ws 自动重连。
func (p ReconnectPolicy) enabled() bool { return p.Delay > 0 }

// maxFailuresOrDefault 返回失败上限(<=0 时用默认)。
func (p ReconnectPolicy) maxFailuresOrDefault() int {
	if p.MaxFailures <= 0 {
		return defaultMaxFailures
	}
	return p.MaxFailures
}

// backoffDelay 返回第 n 次失败后的退避(n>=1):min(base*2^(n-1), maxReconnectDelay)。
func backoffDelay(base time.Duration, n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := base
	for i := 1; i < n && d < maxReconnectDelay; i++ {
		d *= reconnectMultiplier
	}
	if d > maxReconnectDelay {
		d = maxReconnectDelay
	}
	return d
}

// Dialer ..
type Dialer struct {
	bunker.Authenticator
	RefreshTime time.Duration
	NetherNetID string
	Reconnect   ReconnectPolicy
	LogName     string // 房间标识(如"备注(房间号)"),用于 ws 日志
}

// DialContext 建立到 signaling 服务器的连接。首连必须同步成功,随后启动 ws 自动重连与凭据刷新。
func (d Dialer) DialContext(
	ctx context.Context,
	serverBaseAddress string,
	g79UserUID uint32,
	signalingSeed []byte,
	signalingTicket []byte,
) (*Conn, error) {
	if len(d.NetherNetID) == 0 {
		d.NetherNetID = fmt.Sprintf("%d", rand.Uint64())
	}

	c := newConn(d, serverBaseAddress, g79UserUID, signalingSeed, signalingTicket)

	// 首次连接必须同步成功,否则房间创建失败。
	if err := c.openSession(ctx); err != nil {
		c.cancel(fmt.Errorf("DialContext: %v", err))
		return nil, fmt.Errorf("DialContext: %v", err)
	}

	go c.supervise()
	go c.autoRefresh(d.RefreshTime)
	return c, nil
}

// dialWebsocket 用 Conn 保存的拨号参数建立一条新的 ws 连接(首连与重连共用)。
func (c *Conn) dialWebsocket(ctx context.Context) (*websocket.Conn, error) {
	opt := &websocket.DialOptions{
		HTTPClient: new(http.Client),
		HTTPHeader: make(http.Header),
	}
	opt.HTTPHeader.Set("Authorization", "NeteaseSignalingAuthToken")
	opt.HTTPHeader.Set("Request-Id", uuid.New().String())
	opt.HTTPHeader.Set("Session-Id", uuid.New().String())
	opt.HTTPHeader.Set("Sec-WebSocket-Protocol", "")
	opt.HTTPHeader.Set("User-Agent", "okhttp/3.10.0")

	finalAddress := fmt.Sprintf(
		"ws://%s/%s/%d/%s/%s",
		c.serverBaseAddress,
		c.dialer.NetherNetID,
		c.g79UserUID,
		base64.URLEncoding.EncodeToString(c.signalingSeed),
		base64.URLEncoding.EncodeToString(c.signalingTicket),
	)
	conn, _, err := websocket.Dial(ctx, finalAddress, opt)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
