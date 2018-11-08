package sale

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestSale only tests basic pre-processing error-checks for Aggregate functions.
func TestSale(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SaleAggregate Suite")
}

var _ = Describe("SaleAggregate", func() {
	Describe("delete", func() {
		It("should return error if filter is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          []byte("{}"),
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Delete(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})

	Describe("insert", func() {
		var sale *Sale

		BeforeEach(func() {
			saleID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			sale = &Sale{
				SaleID: saleID,
				Items: []SoldItem{
					SoldItem{
						ItemID:  itemID,
						Barcode: "test-barcode",
						Weight:  12.24,
						Lot:     "test-lot",
						SKU:     "test-sku",
					},
				},
				Timestamp: time.Now().Unix(),
			}
		})

		It("should return error if saleID is empty", func() {
			sale.SaleID = uuuid.UUID{}
			marshalSale, err := json.Marshal(sale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if items is empty", func() {
			sale.Items = []SoldItem{}
			marshalSale, err := json.Marshal(sale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if timestamp is empty", func() {
			sale.Timestamp = 0
			marshalSale, err := json.Marshal(sale)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalSale,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})

	Describe("update", func() {
		It("should return error if filter is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{},
				"update": map[string]interface{}{},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if saleID in update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{
					"saleID": (uuuid.UUID{}).String(),
				},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if timestamp in update is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			updateArgs := map[string]interface{}{
				"filter": map[string]interface{}{
					"x": 1,
				},
				"update": map[string]interface{}{
					"timestamp": 0,
				},
			}
			marshalArgs, err := json.Marshal(updateArgs)
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   AggregateID,
				Data:          marshalArgs,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Update(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})
})
