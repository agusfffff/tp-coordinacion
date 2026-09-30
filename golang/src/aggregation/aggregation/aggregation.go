package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[int]map[string]fruititem.FruitItem
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[int]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, clientId, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords)
}

func (aggregation *Aggregation) sendFruitTop(clientId int) error {
	fruitTopRecords := aggregation.buildFruitTop(clientId)

	message, err := inner.SerializeMessage(fruitTopRecords, clientId, false)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	return nil
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId int) error {
	slog.Info("Received End Of Records message", "client", clientId)

	err := aggregation.sendFruitTop(clientId)
	if err != nil {
		return err
	}

	err = aggregation.sendEOF(clientId)
	if err != nil {
		return err
	}

	delete(aggregation.fruitItemMap, clientId)
	return nil
}

func (aggregation *Aggregation) sendEOF(clientId int) error {
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage, clientId, true)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId int, fruitRecords []fruititem.FruitItem) {
	clientRecords, ok := aggregation.fruitItemMap[clientId]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMap[clientId] = clientRecords
	}

	for _, fruitRecord := range fruitRecords {
		if currentFruit, ok := clientRecords[fruitRecord.Fruit]; ok {
			clientRecords[fruitRecord.Fruit] = currentFruit.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId int) []fruititem.FruitItem {
	clientRecords := aggregation.fruitItemMap[clientId]

	fruitItems := make([]fruititem.FruitItem, 0, len(clientRecords))
	for _, item := range clientRecords {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
