package engine

import (
	"fmt"
	"os"
)

func processMeta(command []byte, storage Storage) ExecutionStatus {
	if Equal(command, ".exit") {
		closeStorage(storage)
		os.Exit(0)
	} else {
		return MetaUnrecognizedCommand
	}

	return MetaCommandSuccess
}

func closeStorage(storage Storage) {
	if status, err := storage.Close(); err != nil {
		fmt.Println(err)
		os.Exit(int(status))
	}
}
