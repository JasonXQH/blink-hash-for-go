package blinkhash

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type LabelDelete struct {
	Nodes      [EntryNum]NodeInterface
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

func NewDeletionList() *DeletionList {
	return &DeletionList{}
}

func (d *DeletionList) size() int {
	return d.DeletionListCount
}

func (d *DeletionList) Head() *LabelDelete {
	return d.HeadDeletionList
}

func (d *DeletionList) Add(n NodeInterface, globalEpoch uint64) {
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

func (d *DeletionList) Remove(label, prev *LabelDelete) error {
	if label == nil {
		return fmt.Errorf("label is nil")
	}
	if prev == nil {
		d.HeadDeletionList = label.Next
	} else {
		prev.Next = label.Next
	}
	d.DeletionListCount -= label.NodesCount

	label.Next = d.FreeLabelDeletes
	d.FreeLabelDeletes = label
	d.Deleted += uint64(label.NodesCount)
	return nil
}

//-------------------------------------------
// Epoche 和ThreadInfo
//-------------------------------------------

type Epoche struct {
	CurrentEpoche    uint64
	DeletionLists    sync.Map // thread-specific storage
	StartGCThreshold int
}

type ThreadInfo struct {
	Epoche       *Epoche
	DeletionList *DeletionList
}

// NewEpoche creates a new Epoche instance with the specified StartGCThreshold.
func NewEpoche(startGCThreshold int) *Epoche {
	return &Epoche{
		CurrentEpoche:    0,
		DeletionLists:    sync.Map{},
		StartGCThreshold: startGCThreshold,
	}
}

func NewThreadInfo(epoche *Epoche) *ThreadInfo {
	dl := NewDeletionList()
	ti := &ThreadInfo{
		Epoche:       epoche,
		DeletionList: dl,
	}
	epoche.DeletionLists.Store(ti, dl)
	return ti
}

func (ti *ThreadInfo) GetDeletionList() *DeletionList {
	return ti.DeletionList
}

func (ti *ThreadInfo) GetEpoche() *Epoche {
	return ti.Epoche
}

// EnterEpoche marks the beginning of an epoch for the thread.
func (e *Epoche) EnterEpoche(ti *ThreadInfo) {
	curEpoche := atomic.LoadUint64(&e.CurrentEpoche)
	atomic.StoreUint64(&ti.DeletionList.LocalEpoche, curEpoche)
}

// MarkNodeForDeletion marks a node for deletion.
func (e *Epoche) MarkNodeForDeletion(n NodeInterface, ti *ThreadInfo) {
	currentEpoche := atomic.LoadUint64(&e.CurrentEpoche)
	ti.DeletionList.Add(n, currentEpoche)
	ti.DeletionList.ThresholdCounter++
}

// ExitEpocheAndCleanup marks the end of an epoch and performs cleanup if necessary.
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

		// Set localEpoche to max to indicate no active epoch.
		atomic.StoreUint64(&dl.LocalEpoche, ^uint64(0)) // max uint64

		var oldestEpoche uint64 = ^uint64(0)
		e.DeletionLists.Range(func(key, value interface{}) bool {
			dl := value.(*DeletionList)
			localEpoche := atomic.LoadUint64(&dl.LocalEpoche)
			if localEpoche < oldestEpoche {
				oldestEpoche = localEpoche
			}
			return true
		})

		var prev *LabelDelete
		for cur := dl.Head(); cur != nil; cur = cur.Next {
			if cur.Epoche < oldestEpoche {
				// In Go, memory is managed by GC, so no need to manually delete nodes.
				// However, if you have custom memory management, handle it here.
				dl.Remove(cur, prev)
			} else {
				prev = cur
			}
		}
		dl.ThresholdCounter = 0
	}
}

// ShowDeleteRatio displays the ratio of deleted to added nodes.
func (e *Epoche) ShowDeleteRatio() {
	e.DeletionLists.Range(func(key, value interface{}) bool {
		dl := value.(*DeletionList)
		fmt.Printf("Deleted %d of %d\n", dl.Deleted, dl.Added)
		return true
	})
}

// Cleanup performs a global cleanup of all deletion lists.
func (e *Epoche) Cleanup() {
	var oldestEpoche uint64 = ^uint64(0)
	e.DeletionLists.Range(func(key, value interface{}) bool {
		dl := value.(*DeletionList)
		localEpoche := atomic.LoadUint64(&dl.LocalEpoche)
		if localEpoche < oldestEpoche {
			oldestEpoche = localEpoche
		}
		return true
	})

	e.DeletionLists.Range(func(key, value interface{}) bool {
		dl := value.(*DeletionList)
		var prev *LabelDelete
		for cur := dl.Head(); cur != nil; {
			next := cur.Next
			if cur.Epoche < oldestEpoche {
				// In Go, memory is managed by GC, so no need to manually delete nodes.
				// However, if nodes require custom cleanup, handle here.
				dl.Remove(cur, prev)
			} else {
				prev = cur
			}
			cur = next
		}
		return true
	})
}

//-------------------------------------------
// EpocheGuard 和 EpocheGuardReadOnly
//-------------------------------------------

// EpocheGuard manages the epoch lifecycle for a thread (read-write operations).
type EpocheGuard struct {
	ti *ThreadInfo
}

// NewEpocheGuard creates a new EpocheGuard instance.
// It should be used with defer to ensure Release is called.
func NewEpocheGuard(ti *ThreadInfo) *EpocheGuard {
	ti.Epoche.EnterEpoche(ti)
	return &EpocheGuard{ti: ti}
}

// Release exits the epoch and performs cleanup.
func (eg *EpocheGuard) Release() {
	eg.ti.Epoche.ExitEpocheAndCleanup(eg.ti)
}

// EpocheGuardReadonly manages the epoch lifecycle for a thread (read-only operations).
type EpocheGuardReadonly struct {
	ti *ThreadInfo
}

// NewEpocheGuardReadonly creates a new EpocheGuardReadonly instance.
// It should be used with defer to ensure Release is called.
func NewEpocheGuardReadonly(ti *ThreadInfo) *EpocheGuardReadonly {
	ti.Epoche.EnterEpoche(ti)
	return &EpocheGuardReadonly{ti: ti}
}

// Release exits the epoch without performing cleanup.
func (eg *EpocheGuardReadonly) Release() {
	// No cleanup required for read-only operations.
	// If needed, you can implement specific logic here.
}
