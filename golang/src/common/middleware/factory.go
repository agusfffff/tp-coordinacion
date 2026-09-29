package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrCreateMiddlewareConn    = errors.New("create middleware: conn failed")
	ErrCreateMiddlewareChannel = errors.New("create middleware: create channel failed")
	ErrCreateMiddlewareDeclare = errors.New("create middleware: declare failed")
)

const publishTimeout = 5 * time.Second
const consumerName = "consumer-"
const prefetchMsg = 1

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := dialAndConnectCh(connectionSettings.Hostname, connectionSettings.Port)

	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrCreateMiddlewareDeclare
	}

	return &queueMiddleware{conn: conn, channel: ch, queue: q.Name}, nil
}

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queue     string
	consuming atomic.Bool
	id        string
	mut       sync.Mutex
}

// Close implements [middleware.Middleware].
func (q *queueMiddleware) Close() error {
	return closeChAndConn(q.channel, q.conn)
}

// StartConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if !q.consuming.CompareAndSwap(false, true) {
		return nil
	}

	err := q.channel.Qos(
		prefetchMsg,
		0,
		false,
	)

	if err != nil {
		q.consuming.Store(false)
		return classifyError(err)
	}

	consumerTag := consumerName + q.queue

	msgCh, err := q.channel.Consume(
		q.queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		q.consuming.Store(false)
		return classifyError(err)
	}

	q.mut.Lock()
	q.id = consumerTag
	q.mut.Unlock()

	err = consumeLoop(q.channel, msgCh, callbackFunc)

	q.mut.Lock()
	q.id = ""
	q.mut.Unlock()
	q.consuming.Store(false)
	return err

}

func consumeLoop(ch *amqp.Channel, msgCh <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) error {
	for msgD := range msgCh {
		msg := Message{Body: string(msgD.Body)}

		ack := func() {
			_ = msgD.Ack(false)
		}

		nack := func() {
			_ = msgD.Nack(false, true)
		}

		callbackFunc(msg, ack, nack)
	}

	if ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return nil
}

// StopConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StopConsuming() error {
	if q.channel == nil || q.channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if !q.consuming.Load() {
		return nil
	}

	q.mut.Lock()
	if q.id == "" {
		q.mut.Unlock()
		return nil
	}
	err := q.channel.Cancel(q.id, false)
	q.mut.Unlock()

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareClose
	}

	return nil
}

func (q *queueMiddleware) Send(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)

	defer cancel()

	err := q.channel.PublishWithContext(ctx,
		"",
		q.queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})

	if err != nil {
		return classifyError(err)
	}

	return nil

}

type exchangeMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string
	keys      []string
	consuming atomic.Bool
	id        string
	queue     string
	mut       sync.Mutex
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := dialAndConnectCh(connectionSettings.Hostname, connectionSettings.Port)

	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrCreateMiddlewareDeclare
	}

	return &exchangeMiddleware{conn: conn, channel: ch, exchange: exchange, keys: keys}, nil
}

// Close implements [middleware.Middleware].
func (e *exchangeMiddleware) Close() error {
	return closeChAndConn(e.channel, e.conn)
}

// Send implements [middleware.Middleware].
func (e *exchangeMiddleware) Send(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	for _, key := range e.keys {
		err := e.channel.PublishWithContext(ctx,
			e.exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})

		if err != nil {
			return classifyError(err)
		}
	}

	return nil
}

// StartConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {

	if !e.consuming.CompareAndSwap(false, true) {
		return nil
	}

	err := e.channel.Qos(
		prefetchMsg,
		0,
		false,
	)

	if err != nil {
		e.consuming.Store(false)
		return classifyError(err)
	}

	queueName := e.exchange + "." + strings.Join(e.keys, ".")

	q, err := e.channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		e.consuming.Store(false)
		return classifyError(err)
	}

	queue := q.Name

	err = bindKeys(e.channel, queue, e.exchange, e.keys)
	if err != nil {
		e.consuming.Store(false)
		return err
	}

	consumerTag := consumerName + queue

	msgCh, err := e.channel.Consume(
		queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		e.consuming.Store(false)
		return classifyError(err)
	}

	e.mut.Lock()
	e.id = consumerTag
	e.mut.Unlock()
	e.queue = queue

	err = consumeLoop(e.channel, msgCh, callbackFunc)

	e.mut.Lock()
	e.id = ""
	e.mut.Unlock()
	e.consuming.Store(false)

	return err

}

func bindKeys(channel *amqp.Channel, queue string, exchange string, keys []string) error {
	for _, key := range keys {
		err := channel.QueueBind(
			queue,
			key,
			exchange,
			false,
			nil)

		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

// StopConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StopConsuming() error {

	if e.channel == nil || e.channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if !e.consuming.Load() {
		return nil
	}

	e.mut.Lock()
	if e.id == "" {
		e.mut.Unlock()
		return nil
	}
	err := e.channel.Cancel(e.id, false)
	e.mut.Unlock()

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareClose
	}

	return nil
}

func closeChAndConn(channel *amqp.Channel, conn *amqp.Connection) error {
	chErr := channel.Close()
	conErr := conn.Close()

	if chErr != nil || conErr != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func classifyError(err error) error {
	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}

func dialAndConnectCh(hostname string, port int) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://%s:%d/", hostname, port)
	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, ErrCreateMiddlewareChannel
	}

	return conn, ch, nil
}
