package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

var SUDNYI_TICKER time.Ticker
var MAX_MSG_SIZE = 500

type UUID string
type Push struct {
	FROM_UUID UUID

	TO_UUID UUID
}
type UserLoad struct {
	LastPush time.Time
	Buffer   []Push
	Mtx      sync.Mutex
}

func GetUserLoad(uuid UUID) *UserLoad {
	main_mutex.Lock()
	defer main_mutex.Unlock()
	return push_map[uuid]
}
func InitUserLoad(ctx context.Context, uuid UUID, wg *sync.WaitGroup) *UserLoad {
	main_mutex.Lock()
	defer main_mutex.Unlock()
	load, ok := push_map[uuid]
	if ok {
		fmt.Println("user already exists")
		return load
	}
	load = &UserLoad{
		LastPush: time.Now(),
		Buffer:   make([]Push, 0, 100),
		Mtx:      sync.Mutex{},
	}
	push_map[uuid] = load
	wg.Add(1)
	go func() {
		defer wg.Done()
		load.start_flasher(ctx)

	}()
	return load

}
func just_send() {
	//send

}
func (u *UserLoad) send() {
	just_send()
	u.LastPush = time.Now()
	u.Buffer = u.Buffer[:0]
}
func (u *UserLoad) start_flasher(ctx context.Context) {
	t := time.NewTicker(time.Duration(5) * time.Second)
	for {
		select {
		case <-ctx.Done():

			u.Mtx.Lock()
			if len(u.Buffer) > 0 {
				u.send()
				u.LastPush = time.Now()

			}
			u.Mtx.Unlock()
			return
		case <-t.C:
			u.Mtx.Lock()
			if len(u.Buffer) > 0 {
				u.send()
				u.LastPush = time.Now()

			}
			u.Mtx.Unlock()
		}

	}

}

var push_map = make(map[UUID]*UserLoad)
var main_mutex sync.Mutex
var gap time.Duration = 5 * time.Second

func worker(ctx context.Context, ch chan Push, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():

			return
		case v := <-ch:
			load := GetUserLoad(v.TO_UUID)
			if load == nil {
				load = InitUserLoad(ctx, v.TO_UUID, wg)
				load.Mtx.Lock()
				load.Buffer = append(load.Buffer, v)
				load.Mtx.Unlock()
				continue
			}
			load.Mtx.Lock()
			if len(load.Buffer) >= 100 || load.LastPush.Add(gap).Before(time.Now()) {
				load.Buffer = append(load.Buffer, v)
				load.send()

			} else if load.LastPush.Add(gap).After(time.Now()) {
				load.Buffer = append(load.Buffer, v)

			}
			load.Mtx.Unlock()
		}
	}
}
func finish(ch chan Push) {
	for {
		select {
		case v := <-ch:
			load := push_map[v.TO_UUID]
			if load == nil {
				load = &UserLoad{}
				push_map[v.TO_UUID] = load
			}

			load.Buffer = append(load.Buffer, v)
			if len(load.Buffer) >= MAX_MSG_SIZE {
				load.send()
			}
		default:
			for _, load := range push_map {
				if len(load.Buffer) > 0 {
					load.send()
				}
			}
			return
		}
	}
}
func main() {
	wg := sync.WaitGroup{}
	ch := make(chan Push, 10000)
	ctx, c := signal.NotifyContext(context.Background(), os.Interrupt)
	defer c()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			worker(ctx, ch, &wg)
			wg.Done()
		}()
	}
	cl := make(chan struct{})
	go func() {
		wg.Wait()
		finish(ch)
		close(cl)

	}()
	<-ctx.Done()
	fmt.Println("shutting down, waiting for workers")
	select {
	case <-cl:
		fmt.Println("graceful quit")
	case <-SUDNYI_TICKER.C:
		fmt.Println("force quit")
	}

}
