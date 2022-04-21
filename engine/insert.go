package engine

import (
	"fmt"
)

func prepareInsert(command []byte, statement *Statement) ExecutionStatus {
	row := Row{}
	n, err := fmt.Sscanf(string(command), "insert %d %s %s", &(row.Id), &(row.Username), &(row.Email))
	if n < 3 || err != nil {
		return PrepareSyntaxError
	}
	if len(row.Email) > 255 || len(row.Username) > 255 {
		return PrepareStringTooLong
	}
	if row.Id < 0 {
		return PrepareNegativeId
	}
	statement.RowToInsert = &row

	return PrepareSuccess
}
