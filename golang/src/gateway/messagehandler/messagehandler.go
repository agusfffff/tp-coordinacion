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
	return inner.SerializeData(inner.DataMessage{
		ClientId: messageHandler.clientId,
		Records:  []fruititem.FruitItem{fruitRecord},
	})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOF(inner.EOFMessage{
		ClientId: messageHandler.clientId,
	})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	result, err := inner.DeserializeData(message)
	if err != nil {
		return nil, err
	}

	if result.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return result.Records, nil
}
