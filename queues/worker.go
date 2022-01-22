package queues

import (
	"errors"

	"github.com/Zeedinstein/rabbitmq-go/utils"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	ErrWorkerQueueNotDeclared = errors.New("worker queue not declared")
)

type WorkerQueue struct {
	Name    string
	Channel *amqp.Channel
	Queue   *amqp.Queue
	Errors  chan error
}

func NewWorkerQueue(conn *amqp.Connection, name string) (*WorkerQueue, error) {
	var err error

	queue := new(WorkerQueue)
	queue.Name = name

	queue.Channel, err = conn.Channel()
	if err != nil {
		logrus.Errorf("Failed to create %s: %s", queue.Name, err.Error())
		return nil, err
	}

	q, err := queue.Channel.QueueDeclare(
		queue.Name,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf("Failed to declare %s: %s", queue.Name, err.Error())
		return nil, err
	}

	cancels := queue.Channel.NotifyCancel(make(chan string))
	close := queue.Channel.NotifyClose(make(chan *amqp.Error))
	// listen for any cancel and close events on channel
	go utils.ListenForClose(cancels, close)

	queue.Queue = &q
	queue.Errors = make(chan error)
	return queue, nil
}

func (queue *WorkerQueue) Consume(prefetch int, handler func(<-chan amqp.Delivery)) error {
	if queue.Channel == nil {
		return ErrWorkerQueueNotDeclared
	}

	err := queue.Channel.Qos(prefetch, 0, false)
	if err != nil {
		logrus.Errorf("Failed to set QOS for %s: %s", queue.Name, err.Error())
		return err
	}

	messages, err := queue.Channel.Consume(
		queue.Queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf("Failed to create consumer for %s: %s", queue.Name, err.Error())
		return err
	}
	// Create goroutine for consumer
	go handler(messages)
	logrus.Infof("%s consumer started - waiting for messages", queue.Name)
	err = <-queue.Errors
	logrus.Error(err)
	return err
}

func (queue *WorkerQueue) Publish(message interface{}) error {
	body, err := utils.JSONMarshal(message)
	if err != nil {
		logrus.Errorf("Failed to marshal notification: %v, %s", message, err.Error())
		return err
	}

	err = queue.Channel.Publish(
		"",
		queue.Queue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode:    amqp.Persistent,
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			Body:            body,
		},
	)
	if err != nil {
		logrus.Errorf("Failed to publish notification: %s", err.Error())
		return err
	}

	return nil
}
