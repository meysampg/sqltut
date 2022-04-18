package engine

import (
	"os"
)

func processMeta(command []byte) ExecutionStatus {
	if Equal(command, ".exit") {
		os.Exit(0)
	} else {
		return MetaUnrecognizedCommand
	}

	return MetaCommandSuccess
}
