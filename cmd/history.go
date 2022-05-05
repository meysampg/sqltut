package main

import (
	"bufio"
	"log"
	"os"
	"os/user"
	"sync"
)

// Source from: https://github.com/ysn2233/kafka-prompt/blob/master/kaprompt/history.go

const (
	MaxHistories = 25
)

var (
	mu = &sync.Mutex{}
)

func initHistoryFile() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	err = os.MkdirAll(u.HomeDir+"/.sqltut", 0700)
	if err != nil {
		return "", err
	}
	return u.HomeDir + "/.sqltut/history", nil

}

func Persist(record string) {
	mu.Lock()
	defer mu.Unlock()
	historyFile, err := initHistoryFile()
	if err != nil {
		log.Panic(err)
	}
	f, err := os.OpenFile(historyFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()
	if _, err = f.WriteString(record + "\n"); err != nil {
		log.Panic(err)
	}
}

func LoadHistory() ([]string, error) {
	historyFile, err := initHistoryFile()
	if err != nil {
		return []string{}, err
	}
	if _, err := os.Stat(historyFile); err == nil {
		f, err := os.Open(historyFile)
		if err != nil {
			return []string{}, err
		}
		defer f.Close()
		var histories []string
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			histories = append(histories, scanner.Text())
		}
		histories, err = clearHistory(histories, historyFile)
		if err != nil {
			return histories, err
		}
		return histories, scanner.Err()
	} else {
		return []string{}, nil
	}
}

func clearHistory(histories []string, filename string) ([]string, error) {
	if len(histories) > MaxHistories {
		cleared := histories[len(histories)-MaxHistories:]
		f, err := os.OpenFile(filename, os.O_WRONLY, 0644)
		if err != nil {
			return histories, err
		}
		f.Truncate(0)
		for _, s := range cleared {
			f.WriteString(s + "\n")
		}
		return cleared, nil
	}
	return histories, nil
}
