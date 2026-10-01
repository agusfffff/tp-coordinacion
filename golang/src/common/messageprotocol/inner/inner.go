package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const OpData byte = 'D'
const OpEOF byte = 'E'

var ErrEmptyMessage = errors.New("empty message")
var ErrUnexpectedOpcode = errors.New("unexpected opcode")
var ErrInvalidRecord = errors.New("record is not (fruit, amount)")

type DataMessage struct {
	ClientId int
	Records  []fruititem.FruitItem
}

type EOFMessage struct {
	ClientId int
	SeenBy   []int `json:",omitempty"`
}

type innerMessage struct {
	ClientId int
	Records  [][]interface{}
}

func getPayload(message *middleware.Message, expected byte) ([]byte, error) {
	opcode, err := GetOpcode(message)
	if err != nil {
		return nil, err
	}

	if opcode != expected {
		return nil, ErrUnexpectedOpcode
	}

	return []byte(message.Body[1:]), nil
}

func GetOpcode(message *middleware.Message) (byte, error) {
	if len(message.Body) == 0 {
		return 0, ErrEmptyMessage
	}
	return message.Body[0], nil
}

func SerializeData(msg DataMessage) (*middleware.Message, error) {
	return serialize(OpData, innerMessage{ClientId: msg.ClientId, Records: packRecords(msg.Records)})
}

func SerializeEOF(msg EOFMessage) (*middleware.Message, error) {
	return serialize(OpEOF, msg)
}

func serialize(opcode byte, payload any) (*middleware.Message, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(opcode) + string(body)}, nil
}

func DeserializeData(message *middleware.Message) (DataMessage, error) {
	payload, err := getPayload(message, OpData)
	if err != nil {
		return DataMessage{}, err
	}

	msg := innerMessage{}

	if err := json.Unmarshal(payload, &msg); err != nil {
		return DataMessage{}, err
	}

	records, err := unpackFruitRecords(msg.Records)
	if err != nil {
		return DataMessage{}, err
	}
	return DataMessage{ClientId: msg.ClientId, Records: records}, nil
}

func DeserializeEOF(message *middleware.Message) (EOFMessage, error) {
	payload, err := getPayload(message, OpEOF)
	if err != nil {
		return EOFMessage{}, err
	}

	eof := EOFMessage{}
	err = json.Unmarshal(payload, &eof)
	return eof, err
}

func unpackFruitRecords(records [][]interface{}) ([]fruititem.FruitItem, error) {
	fruitRecords := []fruititem.FruitItem{}
	for _, fruitPair := range records {
		if len(fruitPair) != 2 {
			return nil, ErrInvalidRecord
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, ErrInvalidRecord
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, ErrInvalidRecord
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, nil
}

func packRecords(fruitRecords []fruititem.FruitItem) [][]interface{} {
	data := [][]interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}
	return data
}
