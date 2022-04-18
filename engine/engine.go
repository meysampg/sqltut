package engine

import (
	"bytes"
	"fmt"
)

func Process(command []byte, storage Storage) ExecutionStatus {
	if bytes.HasPrefix(command, []byte(".")) {
		return processMeta(command)
	} else {
		return execute(command, storage)
	}
}

func Equal(a []byte, b string) bool {
	return bytes.Equal(a, []byte(b))
}

func execute(command []byte, storage Storage) uint8 {
	statement, status := PrepareStatement(command)
	if status != PrepareSuccess {
		return status
	}

	switch statement.Type {
	case StatementInsert:
		return storage.Insert(statement.RowToInsert)
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
	}

	return PrepareSuccess
}
