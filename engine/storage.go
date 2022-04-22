package engine

type Storage interface {
	RowNums() uint32
	Insert(row *Row) ExecutionStatus
	Select() ([]*Row, ExecutionStatus)
	Close() (ExecutionStatus, error)
	GetPager() Pager
}
