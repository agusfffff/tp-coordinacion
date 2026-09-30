package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(clienId int, data [][]interface{}, eof bool) ([]byte, error) {
	return json.Marshal(InnerMessage{ClientID: clienId, Records: data, EOF: eof})
}

func deserializeJson(message []byte) (InnerMessage, error) {
	var innerMessage InnerMessage
	err := json.Unmarshal(message, &innerMessage)
	return innerMessage, err
}

func SerializeMessage(fruitRecords []fruititem.FruitItem, id int, eof bool) (*middleware.Message, error) {
	data := [][]interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(id, data, eof)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, int, bool, error) {
	innerMessage, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, 0, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, fruitPair := range innerMessage.Records {
		if len(fruitPair) != 2 {
			return nil, 0, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, 0, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, 0, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, innerMessage.ClientID, innerMessage.EOF, nil
}

type InnerMessage struct {
	ClientID int
	Records  [][]interface{}
	EOF      bool
}
