package engine

type Storage interface {
	Insert(row *Row) ExecutionStatus
	Select() ([]*Row, ExecutionStatus)
	Close() (ExecutionStatus, error)
	GetPager() Pager
	ExecuteMeta(command []byte) ExecutionStatus
}
