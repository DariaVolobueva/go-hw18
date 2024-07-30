package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Orange struct {
    Size float64 `json:"size"`
}

type OrangeCounter struct {
    Small  int
    Medium int
    Large  int
    mu     sync.Mutex
}

func (oc *OrangeCounter) Increment(size float64) {
    oc.mu.Lock()
    defer oc.mu.Unlock()

    if size < 8 {
        oc.Small++
    } else if size < 12 {
        oc.Medium++
    } else {
        oc.Large++
    }
}

func (oc *OrangeCounter) Print() {
    oc.mu.Lock()
    defer oc.mu.Unlock()

    fmt.Printf("Oranges: small=%d, medium=%d, large=%d\n", oc.Small, oc.Medium, oc.Large)
}

func main() {
    config := sarama.NewConfig()
    consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    topic := "oranges"
    partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
    if err != nil {
        panic(err)
    }
    defer partitionConsumer.Close()

    counter := &OrangeCounter{}

    go func() {
        for {
            time.Sleep(10 * time.Second)
            counter.Print()
        }
    }()

    for msg := range partitionConsumer.Messages() {
        var orange Orange
        if err := json.Unmarshal(msg.Value, &orange); err != nil {
            fmt.Println("Error unmarshalling message:", err)
            continue
        }

        counter.Increment(orange.Size)
    }
}