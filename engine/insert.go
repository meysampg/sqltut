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
	statement.RowToInsert = &row

	return PrepareSuccess
}
