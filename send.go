package main

import (
	"log"
	"os"

	// Loads in environment variables
	_ "github.com/joho/godotenv/autoload"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial(os.Getenv("AMQP_URI"))
	failOnError(err, "Failed to connect to RMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	body := `{"title":"Buy cheese and bread for breakfast."}`
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(body)})
	log.Printf(" [x] Sent %s", body)
	failOnError(err, "Failed to publish message")
}
