package engine

type Storage interface {
	Insert(row *Row) ExecutionStatus
}