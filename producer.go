package main

import (
    "encoding/json"
    "fmt"
    "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
    "os"
    "time"
)

type Event struct {
    EventType string `json:"type"`
    EventAction string `json:"action"`
    Version string `json:"version"`
    Source interface{} `json:"source"` //allow any struct to be used as source
}

type EventSource struct {
    source  interface{}
    version string
    name    string
}

type Address struct {
    Street *string `json:"street"`
    StreetAlternate *string `json:"street_alternate"`
    City *string `json:"city"`
    State *string `json:"state"`
    PostalCode *string `json:"postal_code"`
}

type Location struct {
    Lat float64 `json:"lat"`
    Lon float64 `json:"lon"`
}

type Place struct {
    Id string `json:"id"`
    PolygonId  string `json:"polygon_id"`
    OrganizationId *string `json:"organization_id"`
    Name string `json:"name"`
    ChainId *string `json:"chain_id"`
    Location Location `json:"location"`
    Address Address `json:"address"`
    Categories *[]string `json:"categories"`
    Created *time.Time `json:"created"`
    LastModified *time.Time `json:"last_modified"`
    Enabled bool `json:"enabled"`
}

func strPtr(s string) (*string) {
    return &s
}

func serializeEvent(source *EventSource, action string) (out []byte){
    
    message := &Event{
        source.name,
        action,
        source.version,
        &source.source}
    
    serializedMessage, err := json.Marshal(*message)
    if err != nil {
        fmt.Println(err)
        return
    }
    return serializedMessage
}

func sendEvent(targetTopic string, message []byte) {
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
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
    
    
    //include a suffix for the topic depending on the environment
    topic := fmt.Sprintf("%s-%s", targetTopic, os.Getenv("ENVIRONMENT"))
    // Produce messages to topic (asynchronously)
    for _, word := range []string{string(message)} {
        p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          []byte(word),
        }, nil)
    }
    
    // Wait for message deliveries before shutting down
    p.Flush(15 * 1000)
}

/**
Tests
 */
func main() {
    
    //build object for the source of an event
    source := Place{
        "12345",
        "678910",
        strPtr("100"),
        "Cup A Joe",
        strPtr("10000020"),
        Location{98.239, -78.990},
    Address{strPtr("123 main st"), nil, strPtr("Birmingham"), strPtr("AL"), strPtr("22545")},
    nil,
    nil,
        nil,
        true}
    
    //serialize the message and include the EventType, EventAction, and  source version (in case we make any breaking changes)
    message := serializeEvent(&EventSource{&source, "1.0", "Place"}, "Update")

    sendEvent("visit", message)
}
