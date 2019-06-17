package main

import (
    "encoding/json"
    "fmt"
    "github.com/Shopify/sarama"
    "log"
    "os"
    "time"
)

type Event struct {
    EventType   string      `json:"type"`
    EventAction string      `json:"action"`
    Version     string      `json:"version"`
    Source      interface{} `json:"source"` //allow any struct to be used as source
}

type EventSource struct {
    source  interface{}
    version string
    name    string
}

type Address struct {
    Street          *string `json:"street"`
    StreetAlternate *string `json:"street_alternate"`
    City            *string `json:"city"`
    State           *string `json:"state"`
    PostalCode      *string `json:"postal_code"`
}

type Location struct {
    Lat float64 `json:"lat"`
    Lon float64 `json:"lon"`
}

type Place struct {
    Id             string     `json:"id"`
    PolygonId      string     `json:"polygon_id"`
    OrganizationId *string    `json:"organization_id"`
    Name           string     `json:"name"`
    ChainId        *string    `json:"chain_id"`
    Location       Location   `json:"location"`
    Address        Address    `json:"address"`
    Categories     *[]string  `json:"categories"`
    Created        *time.Time `json:"created"`
    LastModified   *time.Time `json:"last_modified"`
    Enabled        bool       `json:"enabled"`
}

type AudienceVersion struct {
    Id string `json:"id"`
    AudienceId string `json:"audience_id"`
    JobStart *time.Time `json:"job_start"`
    JobFinish *time.Time `json:"job_finish"`
    JobFailed *time.Time `json:"job_failed"`
    ParentId *string `json:"parent_id"`
    DeviceCount *int `json:"device_count"`
    FirstAction *time.Time `json:"first_action"`
    DownloadUrl *string `json:"download_url"`
    DownloadUrlExpiration *time.Time `json:"download_url_expiration"`
    Created *time.Time `json:"created"`
    LastModified *time.Time `json:"last_modified"`
    Enabled bool `json:"enabled"`
}

func strPtr(s string) (*string) {
    return &s
}

func serializeEvent(source *EventSource, action string) (out []byte) {
    
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

func sendEvent(topic string, message []byte) {
    producer := newProducer([]string{os.Getenv("KAFKA_SERVER_AND_PORT")})
    
    partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic:"sarama", Value: sarama.StringEncoder(message)})
    
    if err != nil {
        fmt.Println("error!")
    } else {
        // The tuple (topic, partition, offset) can be used as a unique identifier
        // for a message in a Kafka cluster.
        fmt.Printf("Message produced with unique identifier: %s/%d/%d", topic, partition, offset)
    }
    
}


func newProducer(brokerList []string) sarama.SyncProducer {
    
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
    config.Producer.Return.Successes = true
    config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
    
    producer, err := sarama.NewSyncProducer(brokerList, config)
    if err != nil {
        log.Fatalln("Failed to start Sarama producer:", err)
    }
    
    return producer
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
    //
    //sendEvent("visit", message)
    
    sendEvent("sarama", message)
    
}
