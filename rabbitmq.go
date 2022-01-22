// Package rabbitmq connects to the specified host
package rabbitmq

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Consumer interface {
	Consume(prefetch int, handler func(<-chan amqp.Delivery)) error
}

// ConnectionConfig rabbitmq connection configuration object. Used to connect to an instance of rabbitmq
type ConnectionConfig struct {
	Host  string
	Port  string
	User  string
	Pass  string
	VHost string
}

//Rabbit - create rabbit object
type Rabbit struct {
	Connection *amqp.Connection
	Connected  bool
	Close      chan *amqp.Error

	SystemEventsChannel *amqp.Channel
	SystemEventsQueue   *amqp.Queue
}

// NewRabbit creates all needed connections and channels then returns rabbit struct
func NewRabbit(config ConnectionConfig, connectionName string) (*Rabbit, error) {
	var err error
	r := new(Rabbit)
	conf := amqp.Config{
		Properties: amqp.Table{
			"connection_name": connectionName,
		},
	}

	r.Connection, err = amqp.DialConfig(fmt.Sprintf(
		"amqp://%s:%s@%s:%s/%s",
		config.User,
		config.Pass,
		config.Host,
		config.Port,
		config.VHost,
	), conf)
	if err != nil {
		logrus.Errorf("Failed to connect to rabbitmq: %s", err.Error())
		return nil, err
	}
	r.Close = r.Connection.NotifyClose(make(chan *amqp.Error))
	go r.ListenForClose()
	return r, nil
}

func (r *Rabbit) ListenForClose() {
	err := <-r.Close
	logrus.Panic(err.Error())
}
