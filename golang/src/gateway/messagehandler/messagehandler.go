package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID atomic.Int64

type MessageHandler struct {
	clientId int
}

func NewMessageHandler() MessageHandler {
	clientId := int(nextClientID.Add(1))
	return MessageHandler{clientId}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(data, messageHandler.clientId)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(data, messageHandler.clientId)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, clientId, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}
