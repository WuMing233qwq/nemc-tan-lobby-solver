package service

import (
	"time"

	"github.com/Happy2018new/nemc-tan-lobby-solver/protocol/service/signaling"
)

const (
	RoomPrivacyEveryoneCanSee uint8 = iota
	RoomPrivacyOnlyFriendsCanSee
)

const (
	PlayerPermissionVisitor uint32 = iota
	PlayerPermissionMember
	PlayerPermissionOperator
	PlayerPermissionCustom
)

// RoomConfig ..
type RoomConfig struct {
	RoomName         string
	RoomPasscode     string
	RoomPrivacy      uint8
	RoomTagList      []uint8
	RoomRefreshTime  time.Duration
	MaxPlayerCount   uint8
	UsedModItemIDs   []uint64
	PlayerPermission uint32
	AllowPvP         bool

	// Advanced room-create parameters (previously hardcoded inside createTanLobbyRoom).
	GameType     uint8  // Game mode tip
	LevelID      string // Level/map identifier tip
	Voice        int16  // Voice flag tip
	MinLevel     uint32 // Minimum player level requirement
	TeamID       uint64 // Following team id when creating
	MapID        uint64 // Numeric map id
	EnableWebRTC bool   // Whether the room is a WebRTC room
	OwnerPing    uint8  // Host network quality hint
	PerfLv       uint8  // Performance level
}

// DefaultRoomConfig ..
func DefaultRoomConfig(roomName string, roomPasscode string, maxPlayerCount uint8, playerPermission uint32) RoomConfig {
	return RoomConfig{
		RoomName:         roomName,
		RoomPasscode:     roomPasscode,
		RoomPrivacy:      RoomPrivacyEveryoneCanSee,
		RoomTagList:      nil,
		RoomRefreshTime:  signaling.RefreshTimeDefault,
		MaxPlayerCount:   maxPlayerCount,
		UsedModItemIDs:   nil,
		PlayerPermission: playerPermission,
		AllowPvP:         true,

		GameType:     1,
		LevelID:      "7xd2c-PYnXk=",
		Voice:        0,
		MinLevel:     0,
		TeamID:       0,
		MapID:        0,
		EnableWebRTC: true,
		OwnerPing:    3,
		PerfLv:       3,
	}
}

// SetTagList ..
func (r RoomConfig) SetTagList(tagList []uint8) RoomConfig {
	r.RoomTagList = tagList
	return r
}

// SetRefreshTime ..
func (r RoomConfig) SetRefreshTime(duration time.Duration) RoomConfig {
	r.RoomRefreshTime = duration
	return r
}

// SetUsedModItemIDs ..
func (r RoomConfig) SetUsedModItemIDs(itemID []uint64) RoomConfig {
	r.UsedModItemIDs = itemID
	return r
}

// SetAllowPvP ..
func (r RoomConfig) SetAllowPvP(enable bool) RoomConfig {
	r.AllowPvP = enable
	return r
}
