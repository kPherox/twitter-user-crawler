package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/joho/godotenv"
)

func main() {
	bFlag := flag.Bool("b", false, "show progress bar")
	envFile := flag.String("e", ".env", "load env file")
	flag.Parse()

	err := godotenv.Load(*envFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: loading %s file\n", *envFile)
	}

	store, err := NewCrawlerStore()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	defer store.Close()
	offset := store.GetLastOffset()

	var (
		ck = os.Getenv("TW_CONSUMER_KEY")
		cs = os.Getenv("TW_CONSUMER_SECRET")
		at = os.Getenv("TW_ACCESS_TOKEN")
		as = os.Getenv("TW_ACCESS_SECRET")
	)
	if ck == "" || cs == "" || at == "" || as == "" {
		log.Fatal("Error: Not Fonnd OAuth Token")
	}
	client := NewTwitter(ck, cs, at, as)

	rl, err := client.CheckRateLimit()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	rlu := rl.Resources.Users["/users/lookup"]
	max := rlu.Remaining
	if max == 0 {
		end := time.Unix(int64(rlu.Reset), 0)
		diff := end.Sub(time.Now())
		fmt.Printf("Wait to reset rate limit: %v\n", diff)
		time.Sleep(diff)
		rl, err := client.CheckRateLimit()
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		rlu = rl.Resources.Users["/users/lookup"]
		max = rlu.Remaining
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	found := make(chan []twitter.User, max)
	nothing := make(chan int64, max)
	workerErr := make(chan error, max)

	pb := newProgressBar(*bFlag, max)
	defer func() {
		cancel()
	}()
	var rLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	semaphore := make(chan struct{}, rLimit.Cur)
	for max > 0 {
		wg.Add(1)
		go func(offset int64) {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore
				pb.Increment()
				wg.Done()
			}()
			us, err := client.UserLookup(ctx, offset)
			if err != nil {
				workerErr <- err
				return
			}
			if len(us) == 0 {
				nothing <- offset
				return
			}
			found <- us
			return
		}(offset)
		offset += 100
		max -= 1
	}

	go func() {
		for {
			select {
			case err := <-workerErr:
				cancel()
				log.Fatalf("Error: %s", err)
			}
		}
	}()

	hasNothing := false
	if !*bFlag {
		go func() {
			for o := range nothing {
				if !hasNothing {
					fmt.Printf("Nothing: %d..%d", o+1, o+100)
					hasNothing = true
				} else {
					fmt.Printf(", %d..%d", o+1, o+100)
				}
			}
		}()
	}

	wg.Wait()
	if !*bFlag && hasNothing {
		fmt.Println()
	}
	pb.Finish()

	close(nothing)
	close(found)
	count := 0
	ums := make(map[int][]UserModel)
	for us := range found {
		count += len(us)
		for _, u := range us {
			t, _ := time.Parse(time.RubyDate, u.CreatedAt)
			if _, ok := ums[t.Year()]; ok {
				ums[t.Year()] = append(ums[t.Year()], UserModel{u.ID, t})
			} else {
				ums[t.Year()] = []UserModel{UserModel{u.ID, t}}
			}
		}
	}
	if count <= 0 {
		fmt.Println("Not Found user IDs")
		return
	}
	fmt.Printf("Found %d user IDs\n", count)

	if err := os.Mkdir("db", 0755); err != nil && !os.IsExist(err) {
		log.Fatalf("Error: %s", err)
	}
	for y, us := range ums {
		db, err := NewSQLite3(fmt.Sprintf("db/%d.db", y))
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		defer db.Close()
		err = db.BulkInsert(us)
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
	}

	_, err = store.SetLastOffset(offset)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}
