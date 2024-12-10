package blinkhash

// FootprintMetrics 存储footprint计算的各项指标。
type FootprintMetrics struct {
	Meta                     uint64
	StructuralDataOccupied   uint64
	StructuralDataUnoccupied uint64
	KeyDataOccupied          uint64
	KeyDataUnoccupied        uint64
}
