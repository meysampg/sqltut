package engine

type ExecutionStatus = uint8

const (
	PrepareSuccess               ExecutionStatus = 1
	PrepareUnrecognizedStatement ExecutionStatus = 2
	PrepareSyntaxError           ExecutionStatus = 3

	ExecuteSuccess   ExecutionStatus = 10
	ExecuteTableFull ExecutionStatus = 11

	MetaCommandSuccess      ExecutionStatus = 20
	MetaUnrecognizedCommand ExecutionStatus = 21
)
