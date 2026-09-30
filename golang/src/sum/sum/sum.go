package sum

import (
	"fmt"
	"log/slog"
	"sync"

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
	inputQueue       middleware.Middleware
	outputExchange   middleware.Middleware
	fruitItemMap     map[int]map[string]fruititem.FruitItem
	consumerExchange middleware.Middleware
	controlBroadcast middleware.Middleware
	mut              sync.Mutex
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

	controlExchange := config.SumPrefix + "_control"

	allKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		allKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}

	ownKeys := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)

	consumerExchange, err := middleware.CreateExchangeMiddleware(controlExchange, []string{ownKeys}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	controlBroadcast, err := middleware.CreateExchangeMiddleware(controlExchange, allKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		consumerExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:       inputQueue,
		outputExchange:   outputExchange,
		fruitItemMap:     map[int]map[string]fruititem.FruitItem{},
		consumerExchange: consumerExchange,
		controlBroadcast: controlBroadcast,
	}, nil
}

func (sum *Sum) Run() {
	defer sum.closeAll()

	go func() {
		sum.consumerExchange.StartConsuming(sum.handleControlMessage)
	}()

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, clientId, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.broadcastEOFtoSums(msg); err != nil {
			slog.Error("While broadcasting end of record message", "err", err)
			return
		}
		return
	}

	sum.mut.Lock()
	defer sum.mut.Unlock()
	sum.handleDataMessage(clientId, fruitRecords)
}

func (sum *Sum) handleEndOfRecordMessage(clientId int) error {
	slog.Info("Received End Of Records message", "client", clientId)
	err := sum.SendFruitRecords(clientId)
	if err != nil {
		return err
	}

	err = sum.SendEOF(clientId)
	if err != nil {
		return err
	}

	delete(sum.fruitItemMap, clientId)
	return nil
}

func (sum *Sum) SendEOF(clientId int) error {
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage, clientId, true)
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
		message, err := inner.SerializeMessage(fruitRecord, clientId, false)
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

func (sum *Sum) handleDataMessage(clientId int, fruitRecords []fruititem.FruitItem) {
	clientRecords, ok := sum.fruitItemMap[clientId]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[clientId] = clientRecords
	}

	for _, fruitRecord := range fruitRecords {
		if currentFruit, ok := clientRecords[fruitRecord.Fruit]; ok {
			clientRecords[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (sum *Sum) closeAll() {
	middlewares := []middleware.Middleware{
		sum.inputQueue,
		sum.outputExchange,
		sum.consumerExchange,
		sum.controlBroadcast,
	}
	for _, m := range middlewares {
		if err := m.Close(); err != nil {
			slog.Error("Closing middleware", "err", err)
		}
	}
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	_, clientId, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		sum.mut.Lock()
		defer sum.mut.Unlock()
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
}

func (sum *Sum) broadcastEOFtoSums(msg middleware.Message) error {
	if err := sum.controlBroadcast.Send(msg); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}
