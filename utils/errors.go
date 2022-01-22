package utils

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func ListenForClose(cancel chan string, close chan *amqp.Error) {
	select {
	case err := <-cancel:
		logrus.Panic(err)
	case err := <-close:
		logrus.Panic(err)
	}
}
