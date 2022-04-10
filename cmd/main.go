package main

import (
	"bufio"
	"fmt"
	"github.com/meysampg/sqltut/commands"
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
	for {
		prompt()
		l, _ := readInput(reader)
		switch commands.Process(l) {
		case commands.MetaCommandSuccess:
			continue
		case commands.StatementCommandSuccess:
			fmt.Println("Executed.")
		case commands.MetaUnrecognizedCommand:
			fmt.Printf("Unrecognized command '%s'\n", string(l))
		case commands.PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", string(l))
		}
	}
}
