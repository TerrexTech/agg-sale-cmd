package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-sale-cmd/sale"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestSale(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "SaleAggregate Suite")
}

var _ = Describe("SaleAggregate", func() {
	var (
		kafkaBrokers          []string
		eventsTopic           string
		producerResponseTopic string

		mockSale  *sale.Sale
		mockEvent *model.Event
	)
	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		saleID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		itemID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		mockSale = &sale.Sale{
			SaleID: saleID,
			Items: []sale.SoldItem{
				sale.SoldItem{
					ItemID:  itemID,
					Barcode: "test-barcode",
					Weight:  12.24,
					Lot:     "test-lot",
					SKU:     "test-sku",
				},
			},
			Timestamp: time.Now().Unix(),
		}
		marshalSale, err := json.Marshal(mockSale)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		timeUUID, err := uuuid.NewV1()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			Action:        "insert",
			CorrelationID: cid,
			AggregateID:   sale.AggregateID,
			Data:          marshalSale,
			Timestamp:     time.Now(),
			UserUUID:      uid,
			TimeUUID:      timeUUID,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("Sale Operations", func() {
		It("should insert record", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggsale.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					sale := &sale.Sale{}
					err = json.Unmarshal(kr.Result, sale)
					Expect(err).ToNot(HaveOccurred())

					if sale.SaleID == mockSale.SaleID {
						mockSale.ID = sale.ID
						Expect(sale).To(Equal(mockSale))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())

			// Can't search directly in Mongo if using embedded-arrays
			mockFindSale := *mockSale
			mockFindSale.Items = []sale.SoldItem{}
			findResult, err := aggColl.FindOne(mockFindSale)
			commonutil.ErrorStackTrace(err)
			Expect(err).ToNot(HaveOccurred())

			findSale, assertOK := findResult.(*sale.Sale)
			Expect(assertOK).To(BeTrue())
			mockSale.ID = findSale.ID
			Expect(findSale).To(Equal(mockSale))

			close(done)
		}, 2000)

		It("should update record", func(done Done) {
			Byf("Creating update args")
			filterSale := map[string]interface{}{
				"saleID": mockSale.SaleID.String(),
			}
			mockSale.Timestamp = time.Now().Unix()
			// Remove ObjectID because this is not passed from gateway
			mockID := mockSale.ID
			mockSale.ID = objectid.NilObjectID

			mockSale.Timestamp = 1234
			// Can't search directly in Mongo if using embedded-arrays
			mockUpdateSale := *mockSale
			mockUpdateSale.ID = objectid.NilObjectID
			mockUpdateSale.Items = []sale.SoldItem{}
			mockUpdateSale.SaleID = uuuid.UUID{}

			log.Printf("%+v", filterSale)
			log.Printf("%+v", mockUpdateSale)
			update := map[string]interface{}{
				"filter": filterSale,
				"update": &mockUpdateSale,
			}
			marshalUpdate, err := json.Marshal(update)
			Expect(err).ToNot(HaveOccurred())
			// Reassign back ID so we can compare easily with database-entry
			mockSale.ID = mockID

			Byf("Creating update MockEvent")
			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Action = "update"
			mockEvent.Data = marshalUpdate
			mockEvent.Timestamp = time.Now()
			mockEvent.TimeUUID = timeUUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggsale.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["matchedCount"] != 0 && result["modifiedCount"] != 0 {
						Expect(result["matchedCount"]).To(Equal(1))
						Expect(result["modifiedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())

			mockUpdateSaleFind := *mockSale
			mockUpdateSaleFind.Items = []sale.SoldItem{}
			findResult, err := aggColl.FindOne(mockUpdateSaleFind)
			Expect(err).ToNot(HaveOccurred())
			findSale, assertOK := findResult.(*sale.Sale)
			log.Printf("%+v", mockUpdateSale)
			log.Printf("%+v", findSale)
			Expect(assertOK).To(BeTrue())
			Expect(findSale).To(Equal(mockSale))

			close(done)
		}, 2000)

		It("should delete record", func(done Done) {
			Byf("Creating delete args")
			deleteArgs := map[string]interface{}{
				"saleID": mockSale.SaleID,
			}
			marshalDelete, err := json.Marshal(deleteArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating delete MockEvent")
			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Action = "delete"
			mockEvent.Data = marshalDelete
			mockEvent.Timestamp = time.Now()
			mockEvent.TimeUUID = timeUUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggsale.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["deletedCount"] != 0 {
						Expect(result["deletedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			_, err = aggColl.FindOne(mockSale)
			Expect(err).To(HaveOccurred())

			close(done)
		}, 2000)
	})
})
