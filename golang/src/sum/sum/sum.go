package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/shutdown"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                  int
	sumAmount           int
	inputQueue          middleware.Middleware
	aggregatorsExchange []middleware.Middleware
	fruitItemMap        map[int]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange := make([]middleware.Middleware, config.AggregationAmount)

	for i := range config.AggregationAmount {
		routeKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		outputExchange[i], err = middleware.CreateExchangeMiddleware(config.AggregationPrefix, routeKey, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchange[j].Close()
			}
			return nil, err
		}
	}

	return &Sum{
		id:                  config.Id,
		sumAmount:           config.SumAmount,
		inputQueue:          inputQueue,
		aggregatorsExchange: outputExchange,
		fruitItemMap:        map[int]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	defer sum.closeAll()
	shutdown.StopConsumingOnSignal(sum.inputQueue)
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	opcode, err := inner.GetOpcode(&msg)
	if err != nil {
		slog.Error("While reading message opcode", "err", err)
		return
	}

	switch opcode {
	case inner.OpData:
		err = sum.handleDataMessage(&msg)

	case inner.OpEOF:
		err = sum.handleEndOfRecordMessage(&msg)

	default:
		err = inner.ErrUnexpectedOpcode
	}

	if err != nil {
		slog.Error("While handling message", "opcode", string(opcode), "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(msg *middleware.Message) error {
	eof, err := inner.DeserializeEOF(msg)
	if err != nil {
		return err
	}

	if slices.Contains(eof.SeenBy, sum.id) {
		return sum.inputQueue.Send(*msg)
	}

	slog.Info("Received EOF message", "client", eof.ClientId)

	err = sum.sendToAggregators(eof.ClientId)
	if err != nil {
		return err
	}

	delete(sum.fruitItemMap, eof.ClientId)

	eof.SeenBy = append(eof.SeenBy, sum.id)
	if len(eof.SeenBy) == sum.sumAmount {
		return nil
	}
	return sum.requeueEOF(eof)
}

func (sum *Sum) requeueEOF(eof inner.EOFMessage) error {
	message, err := inner.SerializeEOF(eof)
	if err != nil {
		return err
	}
	return sum.inputQueue.Send(*message)
}

func (sum *Sum) sendEOF(clientId int, aggregator middleware.Middleware) error {
	message, err := inner.SerializeEOF(inner.EOFMessage{ClientId: clientId})
	if err != nil {
		return err
	}

	return aggregator.Send(*message)
}

func (sum *Sum) sendToAggregators(clientId int) error {
	recordsByAggregator := make([][]fruititem.FruitItem, len(sum.aggregatorsExchange))

	clientRecords := sum.fruitItemMap[clientId]
	for _, fruitData := range clientRecords {
		i := sum.getAggregatorId(fruitData.Fruit)
		recordsByAggregator[i] = append(recordsByAggregator[i], fruitData)
	}

	for i, aggregator := range sum.aggregatorsExchange {
		err := sum.sendFruitRecords(clientId, aggregator, recordsByAggregator[i])
		if err != nil {
			return err
		}

		err = sum.sendEOF(clientId, aggregator)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sum *Sum) sendFruitRecords(clientId int, aggregator middleware.Middleware, records []fruititem.FruitItem) error {
	if len(records) == 0 {
		return nil
	}

	message, err := inner.SerializeData(inner.DataMessage{ClientId: clientId, Records: records})
	if err != nil {
		return err
	}

	return aggregator.Send(*message)
}

func (sum *Sum) handleDataMessage(msg *middleware.Message) error {
	data, err := inner.DeserializeData(msg)
	if err != nil {
		return err
	}

	clientRecords, ok := sum.fruitItemMap[data.ClientId]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[data.ClientId] = clientRecords
	}

	for _, fruitRecord := range data.Records {
		if currentFruit, ok := clientRecords[fruitRecord.Fruit]; ok {
			clientRecords[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) closeAll() {
	middlewares := append([]middleware.Middleware{sum.inputQueue}, sum.aggregatorsExchange...)

	for _, m := range middlewares {
		err := m.Close()
		if err != nil {
			slog.Error("Closing middleware", "err", err)
		}
	}
}

func (sum *Sum) getAggregatorId(fruit string) int {
	hash := fnv.New32a()
	hash.Write([]byte(fruit))
	return int(hash.Sum32() % uint32(len(sum.aggregatorsExchange)))
}
