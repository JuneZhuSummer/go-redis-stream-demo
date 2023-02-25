package main

import (
	"context"
	"fmt"
	"innovation/go-redis-stream-demo/enum"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	go consume()
	<-quit
}

func ping(ctx context.Context) {
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("redis client connect success!")
}

func consume() {
	ticker := time.NewTicker(time.Second) //去掉ticker即跑满cpu
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r := &redis.XReadArgs{
				Streams: []string{enum.StreamKey, "0"},
				Count:   1,
				Block:   0, //0:阻塞式获取
			}
			entries, err := rdb.XRead(ctx, r).Result()
			if err != nil {
				log.Println(err)
				return
			}

			for _, entry := range entries {
				if entry.Stream == enum.StreamKey {
					for _, msg := range entry.Messages {
						log.Printf("start consume:%+v", msg)
						rdb.XDel(ctx, enum.StreamKey, msg.ID)
					}
				}
			}
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
