package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

type Orange struct {
    Size float64 `json:"size"`
}

func main() {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true

    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    topic := "oranges"

    for {
        orange := Orange{Size: rand.Float64()*10 + 5} // розмір від 5 до 15 см
        orangeJSON, _ := json.Marshal(orange)

        msg := &sarama.ProducerMessage{
            Topic: topic,
            Value: sarama.StringEncoder(orangeJSON),
        }

        _, _, err := producer.SendMessage(msg)
        if err != nil {
            fmt.Println("Error sending message:", err)
        } else {
            fmt.Printf("Sent orange: %.2f cm\n", orange.Size)
        }

        time.Sleep(time.Second)
    }
}