package codec

const (
	kindString  = byte(1)
	kindBytes   = byte(2)
	kindBool    = byte(3)
	kindInt64   = byte(4)
	kindFloat64 = byte(5)
	kindMap     = byte(6)
	kindList    = byte(7)
	kindHash    = byte(8)

	kindCommit     = byte(100)
	kindDataRoot   = byte(101)
	kindCollection = byte(102)
	kindDocument   = byte(103)
)
