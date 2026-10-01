package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/shutdown"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	partialTops       map[int]map[string]fruititem.FruitItem
	topCount          map[int]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		partialTops:       map[int]map[string]fruititem.FruitItem{},
		topCount:          map[int]int{},
	}, nil
}

func (join *Join) Run() error {
	defer join.closeAll()
	shutdown.StopConsumingOnSignal(join.inputQueue)
	return join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	partialTop, err := inner.DeserializeData(&msg)
	if err != nil {
		slog.Error("While deserializing partial top", "err", err)
		return
	}

	join.addPartialTop(partialTop)

	if !join.topCountComplete(partialTop.ClientId) {
		return
	}

	if err := join.sendFinalTop(partialTop.ClientId); err != nil {
		slog.Error("While sending final top", "client", partialTop.ClientId, "err", err)
	}

}

func (join *Join) addPartialTop(partialTop inner.DataMessage) {
	clientId := partialTop.ClientId
	clientFruits, ok := join.partialTops[clientId]
	if !ok {
		clientFruits = map[string]fruititem.FruitItem{}
		join.partialTops[clientId] = clientFruits
	}

	for _, fruitItem := range partialTop.Records {
		clientFruits[fruitItem.Fruit] = fruitItem
	}
}

func (join *Join) topCountComplete(clientId int) bool {
	join.topCount[clientId]++
	return join.topCount[clientId] == join.aggregationAmount
}

func (join *Join) sendFinalTop(clientId int) error {
	finalTop := join.buildFruitTop(clientId)

	message, err := inner.SerializeData(inner.DataMessage{
		ClientId: clientId,
		Records:  finalTop,
	})
	if err != nil {
		return err
	}

	if err := join.outputQueue.Send(*message); err != nil {
		return err
	}

	delete(join.partialTops, clientId)
	delete(join.topCount, clientId)
	return nil
}

func (join *Join) buildFruitTop(clientId int) []fruititem.FruitItem {
	clientRecords := join.partialTops[clientId]

	fruitItems := make([]fruititem.FruitItem, 0, len(clientRecords))
	for _, item := range clientRecords {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (join *Join) closeAll() {
	middlewares := []middleware.Middleware{join.inputQueue, join.outputQueue}

	for _, m := range middlewares {
		err := m.Close()
		if err != nil {
			slog.Error("Closing middleware", "err", err)
		}
	}
}
