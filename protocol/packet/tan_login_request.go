package packet

import "github.com/Happy2018new/nemc-tan-lobby-solver/protocol/encoding"

const (
	PlatformComputer = iota + 1
	PlatformMobile
)

// TanLoginRequest ..
type TanLoginRequest struct {
	PlayerID   uint32
	Rand       []byte
	AESRand    []byte
	PlayerName string
	Platform   uint8
}

func (*TanLoginRequest) ID() uint16 {
	return IDTanLoginRequest
}

func (*TanLoginRequest) BoundType() uint8 {
	return BoundTypeServer
}

func (t *TanLoginRequest) Marshal(io encoding.IO) {
	io.Uint32(&t.PlayerID)
	encoding.FuncSliceOfLen(io, 16, &t.Rand, io.Uint8)
	encoding.FuncSliceOfLen(io, 16, &t.AESRand, io.Uint8)
	io.Uint8String(&t.PlayerName)
	io.Uint8(&t.Platform)
}
