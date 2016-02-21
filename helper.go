package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

func FatalIfError(msg string, err error) {
	if err != nil {
		log.Fatal("\n -- ", msg, " ", err.Error())
	}
}

func StringMapToCassandra(in map[string]string) string {
	// we should find a safe way to do this
	j, _ := json.Marshal(in)
	return strings.Replace(string(j), `"`, `'`, -1)
}

func StringListToArray(in string) []string {
	out := strings.Split(in, ",")
	if len(out) == 1 && out[0] == "" {
		return []string{}
	}
	return out
}

func title(title string) {
	line := strings.Repeat("-", len(title)+6)
	fmt.Println(line)
	fmt.Println("-- " + title + " --")
	fmt.Println(line)
}
