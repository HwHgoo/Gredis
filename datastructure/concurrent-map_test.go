package datastructure

import (
	"testing"
	"time"
)

func TestConcurrentMapBase(t *testing.T) {
	cm := MakeNewConcurrentMap[string]()
	cm.Set("k", "v")
	v, ok := cm.Get("k")
	if !ok {
		t.Log("key not found")
		t.FailNow()
	}

	if v != "v" {
		t.Log("value not match")
		t.FailNow()
	}
	cm.Delete("k")
	v, ok = cm.Get("k")
	if ok || v != "" {
		t.FailNow()
	}
}

func TestConcurrentMapSet(t *testing.T) {
	cm := MakeNewConcurrentMap[string]()
	cancel := make(chan struct{})
	setmap := func() {
		ticker := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case <-ticker.C:
				cm.Set("k", "v")
			case <-cancel:
				return
			}
		}
	}

	for i := 0; i < 10; i++ {
		go setmap()
	}

	time.Sleep(time.Second * 5)
	close(cancel)
}
