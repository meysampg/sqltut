package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/meysampg/sqltut/engine"
	"github.com/meysampg/sqltut/engine/storage/arraylike"
	"github.com/meysampg/sqltut/engine/storage/btree"

	prompt "github.com/c-bata/go-prompt"
)

var (
	dbPath   string
	dbEngine string
)

func init() {
	flag.StringVar(&dbPath, "db-path", "./db", "Path of the DB file")
	flag.StringVar(&dbEngine, "engine", "arraylike", "Engine to store and query")

	flag.Parse()
}

func getEngine(typ, path string) (engine.Storage, error) {
	switch typ {
	case "arraylike":
		return arraylike.DbOpen(path)
	case "btree":
		return btree.DbOpen(path)
	default:
		return nil, fmt.Errorf("Engine not found, %s", typ)
	}
}

func cliOptions() []prompt.Option {
	histories, err := LoadHistory()
	if err != nil {
		log.Printf("Load history fails! %s", err)
	}

	options := []prompt.Option{
		prompt.OptionPrefix(">>> "),
		prompt.OptionInputTextColor(prompt.Yellow),
		prompt.OptionSuggestionBGColor(prompt.Purple),
		prompt.OptionDescriptionBGColor(prompt.Blue),
		prompt.OptionSelectedSuggestionBGColor(prompt.Blue),
		prompt.OptionSelectedDescriptionBGColor(prompt.Purple),
		prompt.OptionSelectedSuggestionTextColor(prompt.Red),
		prompt.OptionHistory(histories),
	}

	return options
}

func completer(in prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "insert", Description: "insert ID username email"},
		{Text: "select", Description: "show all stored users"},
		{Text: ".btree", Description: "show the saved btree (on btree engine)"},
		{Text: ".constants", Description: "show constants (on btree engine)"},
		{Text: ".exit", Description: "flush the db and exit"},
	}
	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}

func main() {
	table, err := getEngine(dbEngine, dbPath)
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(int(engine.ExitFailure))
	}

	for {
		l := prompt.Input(">>> ", completer, cliOptions()...)
		Persist(l)
		switch engine.Process([]byte(l), table) {
		case engine.MetaCommandSuccess:
			continue
		case engine.ExecuteTableFull:
			fmt.Println("Error: Table full.")
			continue
		case engine.PrepareSuccess, engine.ExecuteSuccess:
			fmt.Println("Executed.")
		case engine.MetaUnrecognizedCommand:
			fmt.Printf("Unrecognized command '%s'\n", string(l))
		case engine.PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", string(l))
		case engine.PrepareSyntaxError:
			fmt.Printf("Error on executing `%s`.\n", string(l))
		case engine.PrepareStringTooLong:
			fmt.Println("String is too long.")
		case engine.PrepareNegativeId:
			fmt.Println("ID must be positive.")
		case engine.ExecutePageFetchError:
			os.Exit(int(engine.ExecutePageFetchError))
		case engine.ExecuteDuplicateKey:
			fmt.Println("Error: Duplicate key.")
		}
	}
}
