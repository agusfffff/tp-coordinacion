package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(fruitRecords []fruititem.FruitItem, id int) (*middleware.Message, error) {
	data := [][]interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := json.Marshal(InnerMessage{ClientID: id, Records: data})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, int, bool, error) {
	var innerMessage InnerMessage
	if err := json.Unmarshal([]byte((*message).Body), &innerMessage); err != nil {
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

	return fruitRecords, innerMessage.ClientID, len(fruitRecords) == 0, nil
}

type InnerMessage struct {
	ClientID int
	Records  [][]interface{}
}
