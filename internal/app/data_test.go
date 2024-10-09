package app

import (
	"fmt"
	"testing"
)

func TestStringerWorksOnNodeState(t *testing.T) {
	ns := &NodeState{}
	nsStr := fmt.Sprintf("%v", ns)
	if nsStr != "<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=???>" {
		t.Errorf("%s", ns)
	}

	ns.IsMaster = false
	ns.MasterState = new(MasterState)
	ns.MasterState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-101"
	ns.SlaveState = new(SlaveState)
	ns.SlaveState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40"

	nsStr = fmt.Sprintf("%v", ns)
	if nsStr != "<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40>" {
		t.Errorf("%s", ns)
	}

	ns.ShowOnlyGTIDDiff = true
	nsStr = fmt.Sprintf("%v", ns)
	if nsStr != "<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=source ahead on: 6dbc0b04-4b09-43dc-86cc-9af852ded919:41-101>" {
		t.Errorf("%s", ns)
	}
}

func TestStringerWorksOnNodeStateMap(t *testing.T) {
	m := make(map[string]*NodeState)
	m["a"] = &NodeState{}
	m["b"] = &NodeState{}
	m["c"] = &NodeState{}

	mStr := fmt.Sprintf("%v", m)
	if mStr != "map[a:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=???> b:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=???> c:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=???>]" {
		t.Errorf("%s", mStr)
	}
}
