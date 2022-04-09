package commands

const (
	StatementCommandSuccess ExecutionStatus = 10
)

func processStatement(command []byte) ExecutionStatus {
	return StatementCommandSuccess
}
