package app

import (
	"errors"
	"fmt"
	"time"
)

type appState string

const (
	stateFirstRun    = "FirstRun"
	stateManager     = "Manager"
	stateCandidate   = "Candidate"
	stateLost        = "Lost"
	stateMaintenance = "Maintenance"
)

var (
	ErrNoMaster                 = errors.New("no alive master found")
	ErrManyMasters              = errors.New("more than one master found")
	ErrNoActiveNodes            = errors.New("no active nodes found")
	ErrSwitchoverTimeout        = errors.New("switchover timed out")
	ErrSwitchoverNotAbortable   = errors.New("switchover is not safe to abort")
	ErrSwitchoverAbortRequested = errors.New("switchover safe abort requested")
	ErrSwitchoverTerminal       = errors.New("switchover failure is terminal")
)

const (
	// CauseManual means switchover was issued via command line
	CauseManual = "manual"
	// CauseWorker means switchover was initiated via MDB worker (set directly to dcs)
	CauseWorker = "worker"
	// CauseAuto  means failover was started automatically by failure detection process
	CauseAuto = "auto"
)

type MasterTransition string

const (
	FailoverTransition   MasterTransition = "failover"
	SwitchoverTransition MasterTransition = "switchover"
)

// Switchover contains info about currently running or scheduled switchover/failover process
type Switchover struct {
	From             string            `json:"from"`
	To               string            `json:"to"`
	Cause            string            `json:"cause"`
	InitiatedBy      string            `json:"initiated_by"`
	InitiatedAt      time.Time         `json:"initiated_at"`
	MasterTransition MasterTransition  `json:"master_transition"`
	StartedBy        string            `json:"started_by"`
	StartedAt        time.Time         `json:"started_at"`
	Result           *SwitchoverResult `json:"result"`
	RunCount         int               `json:"run_count,omitempty"`
	OperationID      string            `json:"operation_id,omitempty"`
	Abortable        bool              `json:"abortable,omitempty"`
	TopologyChanged  bool              `json:"topology_changed,omitempty"`
	AbortRequested   bool              `json:"abort_requested,omitempty"`
	AbortRequestedBy string            `json:"abort_requested_by,omitempty"`
	AbortRequestedAt *time.Time        `json:"abort_requested_at,omitempty"`
	DCSVersion       int32             `json:"-"`
}

func (sw *Switchover) String() string {
	var state string
	if sw.Result != nil {
		if sw.Result.Ok {
			state = "done"
		} else {
			state = "ERROR"
		}
	} else if !sw.StartedAt.IsZero() {
		state = "RUNNING"
	} else {
		state = "SCHEDULED"
	}
	swFrom := "*"
	if sw.From != "" {
		swFrom = sw.From
	}
	swTo := "*"
	if sw.To != "" {
		swTo = sw.To
	}
	return fmt.Sprintf("<%s %s=>%s %s by %s at %s>", state, swFrom, swTo, sw.Cause, sw.InitiatedBy, sw.InitiatedAt)
}

// SwitchoverResult contains results of finished/failed switchover
type SwitchoverResult struct {
	Ok         bool      `json:"ok"`
	Error      string    `json:"error"`
	FinishedAt time.Time `json:"finished_at"`
}

type terminalSwitchoverError struct {
	err error
}

func (err terminalSwitchoverError) Error() string {
	return err.err.Error()
}

func (err terminalSwitchoverError) Unwrap() error {
	return err.err
}

func (err terminalSwitchoverError) Is(target error) bool {
	return target == ErrSwitchoverTerminal || errors.Is(err.err, target)
}

func newTerminalSwitchoverError(err error) error {
	return terminalSwitchoverError{err: err}
}

// Maintenance struct presence means that cluster under manual control
// Light mode allows everything except failover and switchover
type MaintenanceMode string

const (
	LightMode MaintenanceMode = "light"
	FullMode  MaintenanceMode = "full"
)

type Maintenance struct {
	InitiatedBy  string          `json:"initiated_by"`
	InitiatedAt  time.Time       `json:"initiated_at"`
	MySyncPaused bool            `json:"mysync_paused"`
	ShouldLeave  bool            `json:"should_leave"`
	Reason       string          `json:"reason,omitempty"`
	Mode         MaintenanceMode `json:"mode"`
}

func (m *Maintenance) MaintAcquired() bool {
	return m != nil && m.MySyncPaused
}

func (m *Maintenance) IsLightMode() bool {
	return m != nil && m.Mode == LightMode
}

func (m *Maintenance) String() string {
	ms := "entering"
	if m.MaintAcquired() {
		ms = "ON"
	}
	if m.ShouldLeave {
		ms = "leaving"
	}
	reasonSuffix := ""
	if m.Reason != "" {
		reasonSuffix = fmt.Sprintf(" (%s)", m.Reason)
	}

	return fmt.Sprintf("<%s by %s at %s%s. mode: %s>", ms, m.InitiatedBy, m.InitiatedAt, reasonSuffix, m.Mode)
}
