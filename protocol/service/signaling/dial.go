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

// Dialer ..
type Dialer struct {
	bunker.Authenticator
	RefreshTime time.Duration
	NetherNetID string
}

// DialContext ..
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
		serverBaseAddress,
		d.NetherNetID,
		g79UserUID,
		base64.URLEncoding.EncodeToString(signalingSeed),
		base64.URLEncoding.EncodeToString(signalingTicket),
	)
	c, _, err := websocket.Dial(ctx, finalAddress, opt)
	if err != nil {
		return nil, err
	}

	conn, err := newConn(ctx, c, d)
	if err != nil {
		return nil, fmt.Errorf("DialContext: %v", err)
	}
	return conn, nil
}
