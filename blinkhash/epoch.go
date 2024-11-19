package blinkhash

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

type LabelDelete struct {
	Nodes      [EntryNum]unsafe.Pointer
	Epoche     uint64
	NodesCount int
	Next       *LabelDelete
}

type DeletionList struct {
	HeadDeletionList  *LabelDelete
	FreeLabelDeletes  *LabelDelete
	DeletionListCount int
	LocalEpoche       uint64
	ThresholdCounter  int
	Deleted           uint64
	Added             uint64
}

type Epoche struct {
	CurrentEpoche    uint64
	DeletionLists    sync.Map // thread-specific storage
	StartGCThreshold int
}

type ThreadInfo struct {
	Epoche       *Epoche
	DeletionList *DeletionList
}

func NewDeletionList() *DeletionList {
	return &DeletionList{}
}

func (d *DeletionList) size() int {
	return d.DeletionListCount
}

func (d *DeletionList) Head() *LabelDelete {
	return d.HeadDeletionList
}

func (d *DeletionList) Add(n unsafe.Pointer, globalEpoch uint64) {
	d.DeletionListCount++
	var label *LabelDelete
	if d.HeadDeletionList != nil && d.HeadDeletionList.NodesCount < EntryNum {
		label = d.HeadDeletionList
	} else {
		if d.FreeLabelDeletes != nil {
			label = d.FreeLabelDeletes
			d.FreeLabelDeletes = d.FreeLabelDeletes.Next
		} else {
			label = &LabelDelete{}
		}
		label.NodesCount = 0
		label.Next = d.HeadDeletionList
		d.HeadDeletionList = label
	}
	label.Nodes[label.NodesCount] = n
	label.NodesCount++
	label.Epoche = globalEpoch

	d.Added++
}

func (d *DeletionList) Remove(label, prev *LabelDelete) {
	if prev == nil {
		d.HeadDeletionList = label.Next
	} else {
		prev.Next = label.Next
	}
	d.DeletionListCount -= label.NodesCount

	label.Next = d.FreeLabelDeletes
	d.FreeLabelDeletes = label
	d.Deleted += uint64(label.NodesCount)
}

func NewThreadInfo(epoche *Epoche) *ThreadInfo {
	return &ThreadInfo{
		Epoche:       epoche,
		DeletionList: NewDeletionList(),
	}
}

func (e *Epoche) EnterEpoche(ti *ThreadInfo) {
	curEpoche := atomic.LoadUint64(&e.CurrentEpoche)
	atomic.StoreUint64(&ti.DeletionList.LocalEpoche, curEpoche)
}

func (e *Epoche) MarkNodeForDeletion(n unsafe.Pointer, ti *ThreadInfo) {
	currentEpoche := atomic.LoadUint64(&e.CurrentEpoche)
	ti.DeletionList.Add(n, currentEpoche)
	ti.DeletionList.ThresholdCounter++
}

func (e *Epoche) ExitEpocheAndCleanup(ti *ThreadInfo) {
	dl := ti.DeletionList
	if dl.ThresholdCounter&(64-1) == 1 {
		atomic.AddUint64(&e.CurrentEpoche, 1)
	}
	if dl.ThresholdCounter > e.StartGCThreshold {
		if dl.DeletionListCount == 0 {
			dl.ThresholdCounter = 0
			return
		}

		atomic.StoreUint64(&dl.LocalEpoche, ^uint64(0)) // max uint64

		var oldestEpoche uint64 = ^uint64(0)
		e.DeletionLists.Range(func(_, value interface{}) bool {
			localEpoche := value.(*DeletionList).LocalEpoche
			if localEpoche < oldestEpoche {
				oldestEpoche = localEpoche
			}
			return true
		})

		var prev *LabelDelete
		for cur := dl.Head(); cur != nil; cur = cur.Next {
			if cur.Epoche < oldestEpoche {
				dl.Remove(cur, prev)
			} else {
				prev = cur
			}
		}
		dl.ThresholdCounter = 0
	}
}

func (e *Epoche) ShowDeleteRatio() {
	e.DeletionLists.Range(func(_, value interface{}) bool {
		dl := value.(*DeletionList)
		fmt.Printf("Deleted %d of %d\n", dl.Deleted, dl.Added)
		return true
	})
}

func (e *Epoche) Cleanup() {
	var oldestEpoche uint64 = ^uint64(0)
	e.DeletionLists.Range(func(_, value interface{}) bool {
		localEpoche := value.(*DeletionList).LocalEpoche
		if localEpoche < oldestEpoche {
			oldestEpoche = localEpoche
		}
		return true
	})

	e.DeletionLists.Range(func(_, value interface{}) bool {
		dl := value.(*DeletionList)
		var prev *LabelDelete
		for cur := dl.Head(); cur != nil; {
			next := cur.Next
			dl.Remove(cur, prev)
			cur = next
		}
		return true
	})
}

func (ti *ThreadInfo) GetDeletionList() *DeletionList {
	return ti.DeletionList
}

func (ti *ThreadInfo) GetEpoche() *Epoche {
	return ti.Epoche
}
