package commands

import (
	"bytes"
	"fmt"
)

const (
	StatementCommandSuccess      ExecutionStatus = 10
	PrepareUnrecognizedStatement ExecutionStatus = 11
)

type StatementType string

const (
	StatementInsert StatementType = "insert"
	StatementSelect StatementType = "select"
)

type Statement struct {
	Type StatementType
}

func processStatement(command []byte) ExecutionStatus {
	statement := prepareStatement(command)
	if statement == nil {
		return PrepareUnrecognizedStatement
	}

	executeStatement(statement)

	return StatementCommandSuccess
}

func executeStatement(statement *Statement) {
	switch statement.Type {
	case StatementInsert:
		fmt.Println("This is where we would do an insert.")
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
	}
}

func prepareStatement(command []byte) *Statement {
	if bytes.HasPrefix(command, []byte("insert")) {
		return &Statement{Type: StatementInsert}
	} else if bytes.HasPrefix(command, []byte("select")) {
		return &Statement{Type: StatementSelect}
	}

	return nil
}
