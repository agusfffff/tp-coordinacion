package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/shutdown"
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
	eofCount      map[int]int
	sumAmount     int
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
		eofCount:      map[int]int{},
		sumAmount:     config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	defer aggregation.closeAll()
	shutdown.StopConsumingOnSignal(aggregation.inputExchange)
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	opcode, err := inner.GetOpcode(&msg)
	if err != nil {
		slog.Error("While reading message opcode", "err", err)
		return
	}

	switch opcode {
	case inner.OpData:
		err = aggregation.handleDataMessage(&msg)

	case inner.OpEOF:
		err = aggregation.handleEndOfRecordMessage(&msg)

	default:
		err = inner.ErrUnexpectedOpcode
	}

	if err != nil {
		slog.Error("While handling message", "opcode", string(opcode), "err", err)
	}
}

func (aggregation *Aggregation) checkEofCount(clientId int) bool {
	aggregation.eofCount[clientId]++
	return aggregation.eofCount[clientId] == aggregation.sumAmount
}

func (aggregation *Aggregation) sendFruitTop(clientId int) error {
	fruitTopRecords := aggregation.buildFruitTop(clientId)

	message, err := inner.SerializeData(inner.DataMessage{
		ClientId: clientId,
		Records:  fruitTopRecords,
	})
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

func (aggregation *Aggregation) handleEndOfRecordMessage(msg *middleware.Message) error {
	eof, err := inner.DeserializeEOF(msg)
	if err != nil {
		return err
	}

	slog.Info("Received End Of Records message", "client", eof.ClientId)

	if !aggregation.checkEofCount(eof.ClientId) {
		return nil
	}

	err = aggregation.sendFruitTop(eof.ClientId)
	if err != nil {
		return err
	}

	delete(aggregation.fruitItemMap, eof.ClientId)
	delete(aggregation.eofCount, eof.ClientId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(msg *middleware.Message) error {
	data, err := inner.DeserializeData(msg)
	if err != nil {
		return err
	}

	clientRecords, ok := aggregation.fruitItemMap[data.ClientId]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMap[data.ClientId] = clientRecords
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

func (aggregation *Aggregation) closeAll() {
	middlewares := []middleware.Middleware{aggregation.inputExchange, aggregation.outputQueue}

	for _, m := range middlewares {
		err := m.Close()
		if err != nil {
			slog.Error("Closing middleware", "err", err)
		}
	}
}
