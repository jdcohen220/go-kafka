package main

import (
    "encoding/json"
    "fmt"
    "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
    "time"
)


type Device struct {
    AdvertiserId string `json:"advertiser_id"`
    Os string `json:"os"`
    HomeLocation *string `json:"home_location"`
    WorkLocation *string `json:"work_location"`
    Created *time.Time `json:"created"`
    LastModified *time.Time `json:"last_modified"`
}

type Event struct {
    EventType string `json:"type"`
    EventAction string `json:"action"`
    Version string `json:"version"`
    Origin *string `json:"origin"`
    Source *Device `json:"source"`
}

func main() {
    
    message := &Event{
        "Device",
        "Update",
        "1.0",
        nil,
        &Device{
            "123abc",
            "android",
            nil,
            nil,
            nil,
            nil}}
    
    serializedMessage, err := json.Marshal(*message)
    if err != nil {
        fmt.Println(err)
        return
    }
    
    fmt.Println(string(serializedMessage))
    
    
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers":
        "b-1.production.51p6rn.c3.kafka.us-east-1.amazonaws.com:9092," +
        "b-3.production.51p6rn.c3.kafka.us-east-1.amazonaws.com:9092," +
        "b-2.production.51p6rn.c3.kafka.us-east-1.amazonaws.com:9092"})
    if err != nil {
        panic(err)
    }
    
    defer p.Close()
    
    // Delivery report handler for produced messages
    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
                }
            }
        }
    }()
    
    // Produce messages to topic (asynchronously)
    topic := "jasonCTestTopic2"
    for _, word := range []string{string(serializedMessage)} {
        p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          []byte(word),
        }, nil)
    }
    
    // Wait for message deliveries before shutting down
    p.Flush(15 * 1000)
}
