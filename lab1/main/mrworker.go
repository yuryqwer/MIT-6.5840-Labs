package main

import (
	"lab1/mr"
	"lab1/mrapps"
)

func main() {
	mr.Worker(mrapps.Map, mrapps.Reduce)
}
