package master

import (
	"gfs"
	"os"
	"testing"
	"time"
)

func TestMaster(t *testing.T) {
	master, err := MakeAndStartMaster(
		gfs.ServerInfo{ServerType: gfs.MASTER, ServerAddr: "localhost:30001"},
		"/tmp/gfs/master",
	)
	if err != nil || master == nil {
		t.Fatal("failed to create master")
	}
	_ = master.Shutdown()
	time.Sleep(1 * time.Second)
	_ = os.RemoveAll("/tmp/gfs/master")
}
