package main

import (
	"fmt"

	"github.com/cheggaaa/pb/v3"
)

type ProgressBar struct {
	use bool
	bar *pb.ProgressBar
}

func newProgressBar(use bool, count int) *ProgressBar {
	var bar *pb.ProgressBar
	if use {
		bar = pb.StartNew(count)
	} else {
		fmt.Println("start...")
	}
	return &ProgressBar{use, bar}
}

func (p *ProgressBar) Increment() {
	if p.use {
		p.bar.Increment()
	}
}

func (p *ProgressBar) Finish() {
	if p.use {
		p.bar.Finish()
	}
	fmt.Println("finish!")
}
