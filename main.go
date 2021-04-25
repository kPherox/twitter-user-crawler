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
	flags := struct {
		printProgressBar bool
		dotenvFilename   string
		consumerKey      string
		consumerSecret   string
		accessToken      string
		accessSecret     string
	}{}
	flag.BoolVar(&flags.printProgressBar, "p", false, "print progress bar")
	flag.StringVar(&flags.dotenvFilename, "env-file", ".env", "load env file")
	flag.StringVar(&flags.consumerKey, "consumer-key", "", "Twitter application consumer key")
	flag.StringVar(&flags.consumerSecret, "consumer-secret", "", "Twitter application consumer secret")
	flag.StringVar(&flags.accessToken, "access-token", "", "Twitter user access token")
	flag.StringVar(&flags.accessSecret, "access-secret", "", "Twitter user access secret")
	flag.Parse()

	err := godotenv.Load(flags.dotenvFilename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: loading %s file\n", flags.dotenvFilename)
	}
	fallbackEnv(&flags.consumerKey, os.Getenv("TW_CONSUMER_KEY"))
	fallbackEnv(&flags.consumerSecret, os.Getenv("TW_CONSUMER_SECRET"))
	fallbackEnv(&flags.accessToken, os.Getenv("TW_ACCESS_TOKEN"))
	fallbackEnv(&flags.accessSecret, os.Getenv("TW_ACCESS_SECRET"))

	store, err := NewCrawlerStore()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	defer store.Close()
	offset := store.GetLastOffset()

	if flags.consumerKey == "" || flags.consumerSecret == "" {
		log.Fatal("Error: Application Access Token required")
	}
	var client *Twitter
	if flags.accessToken == "" || flags.accessSecret == "" {
		client = NewTwitterApp(flags.consumerKey, flags.consumerSecret)
	} else {
		client = NewTwitter(flags.consumerKey, flags.consumerSecret, flags.accessToken, flags.accessSecret)
	}

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

	pb := newProgressBar(flags.printProgressBar, max)
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
			ids := make([]int64, 100)
			var i int64
			for i = 1; i <= 100; i++ {
				ids[i-1] = offset + i
			}

			semaphore <- struct{}{}
			defer func() {
				<-semaphore
				pb.Increment()
				wg.Done()
			}()
			us, err := client.UserLookup(ctx, ids)
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
	if !flags.printProgressBar {
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
	if !flags.printProgressBar && hasNothing {
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

	if err := store.SetLastOffset(offset); err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func fallbackEnv(v *string, fb string) {
	if *v == "" {
		*v = fb
	}
}
