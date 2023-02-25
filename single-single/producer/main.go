package main

import (
	"context"
	"fmt"
	"innovation/go-redis-stream-demo/enum"
	"innovation/go-redis-stream-demo/model"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/go-redis/redis/v8"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan os.Signal
	quit   chan struct{}
	rdb    *redis.Client
)

func init() {
	ctx, cancel = context.WithCancel(context.Background())
	ch = make(chan os.Signal)
	quit = make(chan struct{})
	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", enum.Host, enum.Port),
		Password: enum.Password,
		DB:       enum.DB,
	})
}

func main() {
	go handleSignal()
	ping(ctx)
	go produce(ctx)
	<-quit
}

func ping(ctx context.Context) {
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("redis client connect success!")
}

func produce(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			xlen, err := rdb.XLen(ctx, enum.StreamKey).Result()
			if err != nil {
				log.Println(err)
				return
			}

			if xlen >= enum.MaxLen {
				log.Println("stream overflow!")
				return
			}

			id, err := rdb.Incr(ctx, enum.IncrKey).Result()
			if err != nil {
				log.Println(err)
				return
			}

			msg := model.Message{
				Event: enum.RegisterEvent,
				Data: model.RegisterData{
					Id:   id,
					Name: fmt.Sprintf("june-%d", id),
				},
			}

			msgJson, err := jsoniter.Marshal(msg)
			if err != nil {
				log.Println(err)
				return
			}

			if err = rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: enum.StreamKey,
				Values: map[string]interface{}{
					"message": string(msgJson),
				},
			}).Err(); err != nil {
				log.Println(err)
				return
			}
			log.Printf("produce one message:%+v\n", msg)
		}
	}
}

func handleSignal() {
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	select {
	case sig := <-ch:
		switch sig {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL:
			log.Println("Upon receiving the signal, perform smooth exit!")
			cancel()
			quit <- struct{}{}
		}
	}
}
