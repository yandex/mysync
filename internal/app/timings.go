package app

import "time"

type Timings struct {
	m map[TimingType]map[string]time.Time
}

type TimingType string

const (
	NodeFailedAt       TimingType = "nodeFailedAt"
	StreamFromFailedAt TimingType = "streamFromFailedAt"
	MasterStuckAt      TimingType = "masterStuckAt"
)

func NewTimings() *Timings {
	t := &Timings{}
	t.m = make(map[TimingType]map[string]time.Time)
	t.m[NodeFailedAt] = make(map[string]time.Time)
	t.m[StreamFromFailedAt] = make(map[string]time.Time)
	t.m[MasterStuckAt] = make(map[string]time.Time)
	return t
}

func (t *Timings) Get(tt TimingType, h string) time.Time {
	return t.m[tt][h]
}

func (t *Timings) Set(tt TimingType, h string, v time.Time) {
	t.m[tt][h] = v
}

func (t *Timings) SetIfZero(tt TimingType, h string, v time.Time) {
	if t.m[tt][h].IsZero() {
		t.m[tt][h] = v
	}
}

func (t *Timings) Clean(tt TimingType, h string) {
	t.m[tt][h] = time.Time{}
}
