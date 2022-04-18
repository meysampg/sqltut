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

func execute(command []byte, storage Storage) ExecutionStatus {
	statement, status := PrepareStatement(command)
	if status != PrepareSuccess {
		return status
	}

	switch statement.Type {
	case StatementInsert:
		return storage.Insert(statement.RowToInsert)
	case StatementSelect:
		result, status := storage.Select()
		if status == ExecuteSuccess {
			for _, row := range result {
				fmt.Printf("%#v\n", row)
			}
		}
		return status
	}

	return PrepareSuccess
}
