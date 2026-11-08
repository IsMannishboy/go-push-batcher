package main

import (
        "context"
        "fmt"
        "sync"
        "testing"
        "time"
)

func TestRace(t *testing.T) {
        ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
        defer cancel()

        var wg sync.WaitGroup
        ch := make(chan Push, 10000)

        for i := 0; i < 10; i++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        worker(ctx, ch, &wg)
                }()
        }

        for i := 0; i < 50000; i++ {
                ch <- Push{
                        FROM_UUID: "sender",
                        TO_UUID:   UUID(fmt.Sprintf("user-%d", i%1000)),
                }
        }

        wg.Wait()
}
