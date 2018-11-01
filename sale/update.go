package sale

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type saleUpdate struct {
	Filter map[string]interface{} `json:"filter"`
	Update map[string]interface{} `json:"update"`
}

type updateResult struct {
	MatchedCount  int64 `json:"matchedCount,omitempty"`
	ModifiedCount int64 `json:"modifiedCount,omitempty"`
}

// Update handles "update" events.
func Update(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	saleUpdate := &saleUpdate{}

	err := json.Unmarshal(event.Data, saleUpdate)
	if err != nil {
		err = errors.Wrap(err, "Update: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	update := saleUpdate.Update

	if len(saleUpdate.Filter) == 0 {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}
	if len(update) == 0 {
		err = errors.New("blank update provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	if update["saleID"] != nil && update["saleID"] == (uuuid.UUID{}) {
		err = errors.New("missing saleID")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	// Get Timestamp if present, assert it to Int64, and check if its 0.
	if update["timestamp"] != nil {
		timestamp, err := commonutil.AssertInt64(update["timestamp"])
		if err != nil {
			err = errors.New("error asserting Timestamp")
			err = errors.Wrap(err, "Update")
			log.Println(err)
		}
		if err == nil && timestamp == int64(0) {
			err = errors.New("missing Timestamp")
			err = errors.Wrap(err, "Update")
			log.Println(err)
		}
		if err != nil {
			return &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     InternalError,
				UUID:          event.TimeUUID,
			}
		}
	}

	updateStats, err := collection.UpdateMany(saleUpdate.Filter, update)
	if err != nil {
		err = errors.Wrap(err, "Update: Error in UpdateMany")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			UUID:          event.TimeUUID,
		}
	}

	result := &updateResult{
		MatchedCount:  updateStats.MatchedCount,
		ModifiedCount: updateStats.ModifiedCount,
	}
	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Update: Error marshalling Sale Update-result")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        resultMarshal,
		UUID:          event.TimeUUID,
	}
}
