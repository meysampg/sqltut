package commands

import (
	"os"
)

const (
	MetaCommandSuccess      ExecutionStatus = 20
	MetaUnrecognizedCommand ExecutionStatus = 21
)

func processMeta(command []byte) ExecutionStatus {
	if equal(command, ".exit") {
		os.Exit(0)
	} else {
		return MetaUnrecognizedCommand
	}

	return MetaCommandSuccess
}
