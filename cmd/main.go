package main

import (
	"bufio"
	"fmt"
	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/storage/arraylike"
	"os"
)

func prompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) ([]byte, error) {
	var result []byte
	var isPrefix bool = true

	for isPrefix {
		l, prefix, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		isPrefix = prefix
		result = append(result, l...)
	}

	return result, nil
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	table := arraylike.NewTable()

	for {
		prompt()
		l, _ := readInput(reader)
		switch engine.Process(l, table) {
		case engine.MetaCommandSuccess:
			continue
		case engine.ExecuteTableFull:
			fmt.Println("Table is full.")
			continue
		case engine.PrepareSuccess, engine.ExecuteSuccess:
			fmt.Println("Executed.")
		case engine.MetaUnrecognizedCommand:
			fmt.Printf("Unrecognized command '%s'\n", string(l))
		case engine.PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", string(l))
		case engine.PrepareSyntaxError:
			fmt.Printf("Error on executing `%s`.\n", string(l))
		}
	}
}
