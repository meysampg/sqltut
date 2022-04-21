package engine

type ExecutionStatus = uint16

const (
	PrepareSuccess               ExecutionStatus = 0xA01
	PrepareUnrecognizedStatement ExecutionStatus = 0xA02
	PrepareSyntaxError           ExecutionStatus = 0xA03
	PrepareStringTooLong         ExecutionStatus = 0xA04
	PrepareNegativeId            ExecutionStatus = 0xA05

	ExecuteSuccess     ExecutionStatus = 0xB01
	ExecuteTableFull   ExecutionStatus = 0xB02
	ExecuteTableEmpty  ExecutionStatus = 0xB03
	ExecuteRowNotFound ExecutionStatus = 0xB04

	MetaCommandSuccess      ExecutionStatus = 0xC01
	MetaUnrecognizedCommand ExecutionStatus = 0xC02
)
