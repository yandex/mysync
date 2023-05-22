package dcs

import (
	"errors"
	"strings"
	"time"
)

/*
DCS is the main interface representing data store
DCS implementation should maintain connection to a server,
track connection status changes (connected/disconnected)
and perform basic operations
*/
type DCS interface {
	IsConnected() bool
	WaitConnected(timeout time.Duration) bool
	Initialize() // Create initial data structure if not exists
	SetDisconnectCallback(callback func() error)
	AcquireLock(path string) bool
	ReleaseLock(path string)
	Create(path string, value interface{}) error
	CreateEphemeral(path string, value interface{}) error
	Set(path string, value interface{}) error
	SetEphemeral(path string, value interface{}) error
	Get(path string, dest interface{}) error
	Delete(path string) error
	GetTree(path string) (interface{}, error)
	GetChildren(path string) ([]string, error)
	Close()
}

var (
	// ErrExists means that node being created already exists
	ErrExists = errors.New("key already exists")
	// ErrNotFound means that requested not does not exist
	ErrNotFound = errors.New("key was not found in DCS")
	// ErrMalformed means that we failed to unmarshall received data
	ErrMalformed = errors.New("failed to parse DCS value, possibly data format changed")
)

// sep is a path separator for most common DCS
// Zookeeper, etcd and consul use slash
const (
	sep          = "/"
	sepRune      = '/'
	etcdLockName = "etcd_lock"
)

// LockOwner contains info about the process holding the lock
type LockOwner struct {
	Hostname string `json:"hostname"`
	Pid      int    `json:"pid"`
}

// JoinPath build node path from chunks
func JoinPath(parts ...string) string {
	return strings.Join(parts, sep)
}
