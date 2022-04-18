package engine

import (
	"bytes"
)

type StatementType string

const (
	StatementInsert StatementType = "insert"
	StatementSelect StatementType = "select"
)

type Statement struct {
	Type        StatementType
	RowToInsert *Row
}

func PrepareStatement(command []byte) (*Statement, ExecutionStatus) {
	if bytes.HasPrefix(command, []byte(StatementInsert)) {
		statement := &Statement{Type: StatementInsert}
		return statement, prepareInsert(command, statement)
	} else if bytes.HasPrefix(command, []byte(StatementSelect)) {
		return &Statement{Type: StatementSelect}, PrepareSuccess
	}

	return nil, PrepareUnrecognizedStatement
}
