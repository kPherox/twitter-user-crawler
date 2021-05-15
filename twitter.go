package main

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/dghubble/sling"
)

type Twitter struct {
	client *sling.Sling
}

func NewTwitterApp(ck, cs string) *Twitter {
	config := &clientcredentials.Config{
		ClientID:     ck,
		ClientSecret: cs,
		TokenURL:     "https://api.twitter.com/oauth2/token",
	}
	ctx := context.WithValue(oauth2.NoContext, oauth2.HTTPClient, http.Client{
		Transport: &http2.Transport{},
	})
	httpClient := config.Client(ctx)
	return &Twitter{sling.New().Client(httpClient).Base("https://api.twitter.com/1.1/")}
}

func NewTwitter(ck, cs, at, as string) *Twitter {
	config := oauth1.NewConfig(ck, cs)
	token := oauth1.NewToken(at, as)
	ctx := context.WithValue(oauth1.NoContext, oauth1.HTTPClient, http.Client{
		Transport: &http2.Transport{},
	})
	httpClient := config.Client(ctx, token)
	return &Twitter{sling.New().Client(httpClient).Base("https://api.twitter.com/1.1/")}
}

func (t *Twitter) CheckRateLimit() (rl *twitter.RateLimit, err error) {
	for {
		var apiError *twitter.APIError
		rl, apiError, err = t.checkRateLimit()
		if err != nil {
			if urlError, ok := err.(*url.Error); ok && urlError.Error() == "net/http: TLS handshake timeout" {
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				return
			}
		}
		if apiError == nil || apiError.Empty() {
			return
		}
		if !apiError.Empty() {
			err = apiError
			return
		}
	}
}

func (t *Twitter) checkRateLimit() (rl *twitter.RateLimit, apiError *twitter.APIError, err error) {
	rl = new(twitter.RateLimit)
	apiError = new(twitter.APIError)
	req, err := t.client.New().Get("application/rate_limit_status.json").QueryStruct(&twitter.RateLimitParams{Resources: []string{"users"}}).Request()
	if err != nil {
		return
	}

	_, err = t.client.Do(req, rl, apiError)
	if err != nil {
		return
	}

	return
}

func (t *Twitter) UserLookup(ctx context.Context, ids []int64) (us []twitter.User, err error) {
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
			var apiError *twitter.APIError
			us, apiError, err = t.userLockup(ctx, ids)
			if err != nil {
				return
			}
			if apiError == nil || apiError.Empty() {
				return
			}
			errDetail := apiError.Errors[0]
			if errDetail.Code == 17 {
				return
			}
			if errDetail.Code == 88 {
				err = apiError
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (t *Twitter) userLockup(ctx context.Context, ids []int64) (us []twitter.User, apiError *twitter.APIError, err error) {
	req, err := t.client.New().Get("users/lookup.json").QueryStruct(&twitter.UserLookupParams{UserID: ids}).Request()
	if err != nil {
		return
	}
	req = req.WithContext(ctx)

	users := new([]twitter.User)
	apiError = new(twitter.APIError)
	_, err = t.client.Do(req, users, apiError)
	us = *users
	return
}
