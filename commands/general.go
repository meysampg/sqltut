package commands

import "bytes"

type ExecutionStatus uint8

func Process(command []byte) ExecutionStatus {
	if bytes.HasPrefix(command, []byte(".")) {
		return processMeta(command)
	} else {
		return processStatement(command)
	}
}

func equal(a []byte, b string) bool {
	return bytes.Equal(a, []byte(b))
}
