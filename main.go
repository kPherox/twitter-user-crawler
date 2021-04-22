package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	bFlag := flag.Bool("b", false, "show progress bar")
	envFile := flag.String("e", ".env", "load env file")
	flag.Parse()
	err := godotenv.Load(*envFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading %s file\n", *envFile)
	}

	max := 90000
	pb := newProgressBar(*bFlag, max)
	for max > 0 {
		pb.Increment()
		max--
	}
	pb.Finish()
}
