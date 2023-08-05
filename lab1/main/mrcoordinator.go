package main

import (
	"fmt"
	"lab1/mr"
	"lab1/mrapps"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], mrapps.NREDUCE)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
