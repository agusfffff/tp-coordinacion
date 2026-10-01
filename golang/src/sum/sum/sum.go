package sum

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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
	id             int
	sumAmount      int
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[int]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		id:             config.Id,
		sumAmount:      config.SumAmount,
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[int]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	defer sum.closeAll()

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

	slog.Info("Received End Of Records message", "client", eof.ClientId)

	err = sum.SendFruitRecords(eof.ClientId)
	if err != nil {
		return err
	}

	err = sum.SendEOF(eof.ClientId)
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

func (sum *Sum) SendEOF(clientId int) error {
	message, err := inner.SerializeEOF(inner.EOFMessage{ClientId: clientId})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) SendFruitRecords(clientId int) error {
	clientRecords := sum.fruitItemMap[clientId]
	for _, fruitData := range clientRecords {
		fruitRecord := []fruititem.FruitItem{fruitData}
		message, err := inner.SerializeData(inner.DataMessage{
			ClientId: clientId,
			Records:  fruitRecord,
		})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	return nil
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
	middlewares := []middleware.Middleware{
		sum.inputQueue,
		sum.outputExchange,
	}
	for _, m := range middlewares {
		if err := m.Close(); err != nil {
			slog.Error("Closing middleware", "err", err)
		}
	}
}
