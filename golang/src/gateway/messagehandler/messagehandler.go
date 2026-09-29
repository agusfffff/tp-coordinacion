package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID atomic.Int64

type MessageHandler struct {
	clientID int
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientID: int(nextClientID.Add(1))}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(data, messageHandler.clientID)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(data, messageHandler.clientID)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, clientID, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if clientID != messageHandler.clientID {
		return nil, nil
	}
	return fruitRecords, nil
}
