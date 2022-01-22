package exchanges

import (
	"errors"

	"github.com/Zeedinstein/rabbitmq-go/utils"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	ErrTopicExchangeQueueNotDeclared = errors.New("topic queue is not declared")
	ErrRoutingKeyRequired            = errors.New("routing key required to publish message")
)

type TopicExchange struct {
	Channel      *amqp.Channel
	Queue        *amqp.Queue
	Errors       chan error
	exchangeName string
	queueName    string
	bindings     []string
	durable      bool
}

func NewTopicExchange(conn *amqp.Connection, exchange string, queueName string, durable bool) (*TopicExchange, error) {
	var err error

	ex := new(TopicExchange)
	ex.exchangeName = exchange
	ex.queueName = queueName
	ex.durable = durable

	ex.Channel, err = conn.Channel()
	if err != nil {
		logrus.Errorf("Failed to create %s exchange channel: %s", exchange, err.Error())
		return nil, err
	}

	err = ex.Channel.ExchangeDeclare(
		ex.exchangeName,
		"topic",
		ex.durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf("Failed to declare %s exchange: %s", exchange, err.Error())
		return nil, err
	}

	cancels := ex.Channel.NotifyCancel(make(chan string))
	close := ex.Channel.NotifyClose(make(chan *amqp.Error))
	// listen for any cancel and close events on channel
	go utils.ListenForClose(cancels, close)

	ex.Errors = make(chan error)
	return ex, nil
}

func (ex *TopicExchange) Consume(prefetch int, autoAck bool, handler func(<-chan amqp.Delivery)) error {
	if ex.Channel == nil {
		return ErrTopicExchangeQueueNotDeclared
	}

	queue, err := ex.Channel.QueueDeclare(
		ex.queueName,
		ex.durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf("Failed to declare %s queue for %s exchange: %s", ex.queueName, ex.exchangeName, err.Error())
		return err
	}

	ex.Queue = &queue

	err = ex.Channel.Qos(prefetch, 0, false)
	if err != nil {
		logrus.Errorf("Failed to set QOS for %s: %s", queue.Name, err.Error())
		return err
	}

	messages, err := ex.Channel.Consume(
		ex.queueName, // queue
		"",           // consumer
		autoAck,      // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		logrus.Error(err)
		return err
	}
	// Create separate goroutine for handler
	go handler(messages)
	logrus.Infof("%s exchange consumer started - waiting for messages", ex.exchangeName)
	err = <-ex.Errors
	logrus.Error(err)
	return err
}

func (ex *TopicExchange) Publish(routingKey string, message interface{}) error {
	if routingKey == "" {
		return ErrRoutingKeyRequired
	}
	body, err := utils.JSONMarshal(message)
	if err != nil {
		logrus.Errorf("TopicExchangeError[%s]: %s", ex.exchangeName, err)
		return err
	}
	err = ex.Channel.Publish(
		ex.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			Body:            body,
		},
	)
	if err != nil {
		logrus.Errorf("TopicExchangeError[%s]: %s", ex.exchangeName, err)
		return err
	}
	return nil
}

func (ex *TopicExchange) BindingKeys() []string {
	return ex.bindings
}

func (ex *TopicExchange) BindRoutingKeys(keys ...string) error {
	if len(keys) == 0 {
		return errors.New("BindWithRoutingKeyErr No keys provided")
	}

	// if key exists
	//		do nothing
	// if key does not exist
	//		bind key
	// if bind does not exist in keys
	// 		unbind key

	for _, key := range keys {
		// Check if key already exists in bindings
		if !stringInSlice(key, ex.bindings) {
			logrus.Info("BINDING KEY: ", key)
			err := ex.Channel.QueueBind(
				ex.queueName,
				key,
				ex.exchangeName,
				false,
				nil,
			)
			if err != nil {
				return err
			}
			ex.bindings = append(ex.bindings, key)
		}
	}

	for _, bind := range ex.bindings {
		if !stringInSlice(bind, keys) {
			logrus.Info("UNBINDING KEY: ", bind)
			err := ex.Channel.QueueUnbind(
				ex.queueName,
				bind,
				ex.exchangeName,
				nil,
			)
			if err != nil {
				return err
			}
			ex.bindings = removeFromSlice(bind, ex.bindings)
		}
	}

	return nil
}

func stringInSlice(x string, list []string) bool {
	for _, y := range list {
		if y == x {
			return true
		}
	}
	return false
}

func removeFromSlice(key string, slice []string) []string {
	for at := range slice {
		if slice[at] == key {
			slice = append(slice[:at], slice[at+1:]...)
			break
		}
	}
	return slice
}
