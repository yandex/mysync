package app

import (
	"fmt"
	"testing"
)

func TestStringerWorksOnNodeState(t *testing.T) {
	ns := &NodeState{}
	ns_str := fmt.Sprintf("%s", ns)
	if ns_str != "<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=???>" {
		t.Errorf("%s", ns)
	}

	ns.IsMaster = false
	ns.MasterState = new(MasterState)
	ns.MasterState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-101"
	ns.SlaveState = new(SlaveState)
	ns.SlaveState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40"

	ns_str = fmt.Sprintf("%s", ns)
	if ns_str != "<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40>" {
		t.Errorf("%s", ns)
	}

	ns.ShowOnlyGTIDDiff = true
	ns_str = fmt.Sprintf("%s", ns)
	if ns_str != "<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=source ahead on: 6dbc0b04-4b09-43dc-86cc-9af852ded919:41-101>" {
		t.Errorf("%s", ns)
	}
}
