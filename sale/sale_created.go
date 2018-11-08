package sale

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

func saleCreated(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	sale := &Sale{}
	err := json.Unmarshal(event.Data, sale)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if sale.SaleID == (uuuid.UUID{}) {
		err = errors.New("missing SaleID")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if len(sale.Items) == 0 {
		err = errors.New("missing SaleItems")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if sale.Timestamp == 0 {
		err = errors.New("missing Timestamp")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	marshalItems, err := json.Marshal(sale)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling SaleItems")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating UUID")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	cid := event.CorrelationID
	if cid == (uuuid.UUID{}) {
		cid, err = uuuid.NewV4()
		err = errors.Wrap(err, "Error generating UUID")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	log.Println("99999999999999999999")
	mm := map[string]interface{}{}
	json.Unmarshal(marshalItems, &mm)
	e := model.Event{
		AggregateID:   2,
		CorrelationID: cid,
		EventAction:   "update",
		ServiceAction: "createSale",
		Data:          marshalItems,
		NanoTime:      time.Now().UnixNano(),
		UUID:          uuid,
		Version:       0,
		YearBucket:    2018,
	}

	if producer == nil {
		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: *commonutil.ParseHosts(kafkaBrokersStr),
		})
		if err != nil {
			err = errors.Wrap(err, "ValidateSale: Error creating producer")
			log.Println(err)
			return &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     InternalError,
				EventAction:   event.EventAction,
				ServiceAction: event.ServiceAction,
				UUID:          event.UUID,
			}
		}
	}

	topic := os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
	de, _ := json.Marshal(e)
	log.Println(string(de))
	producer.Input() <- kafka.CreateMessage(topic, de)
	return nil
}
