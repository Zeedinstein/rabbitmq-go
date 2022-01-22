package exchanges

import (
	"github.com/Zeedinstein/rabbitmq-go/utils"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type FanoutExchange struct {
	Channel      *amqp.Channel
	Queue        *amqp.Queue
	Errors       chan error
	exchangeName string
	durable      bool
}

func NewFanoutExchange(conn *amqp.Connection, exchange string, durable bool) (*FanoutExchange, error) {
	var err error

	fanout := new(FanoutExchange)
	fanout.exchangeName = exchange
	fanout.durable = durable
	fanout.Channel, err = conn.Channel()
	if err != nil {
		logrus.Errorf("Failed to create %s exchange channel: %s", exchange, err.Error())
		return nil, err
	}

	err = fanout.Channel.ExchangeDeclare(
		fanout.exchangeName,
		"fanout",
		fanout.durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf("Failed to declare %s exchange: %s", exchange, err.Error())
		return nil, err
	}

	cancels := fanout.Channel.NotifyCancel(make(chan string))
	close := fanout.Channel.NotifyClose(make(chan *amqp.Error))
	// listen for any cancel and close events on channel
	go utils.ListenForClose(cancels, close)

	fanout.Errors = make(chan error)
	return fanout, nil
}

func (fanout *FanoutExchange) Publish(body []byte) error {
	err := fanout.Channel.Publish(
		fanout.exchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			Body:            body,
		},
	)
	if err != nil {
		return err
	}
	return nil
}
