package sale

import (
	"encoding/json"

	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// AggregateID is the global AggregateID for Sale Aggregate.
const AggregateID int8 = 3

// Sale defines the Sale Aggregate.
type Sale struct {
	ID        objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	SaleID    uuuid.UUID        `bson:"saleID,omitempty" json:"saleID,omitempty"`
	Items     []SoldItem        `bson:"items,omitempty" json:"items,omitempty"`
	Timestamp int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
}

// SoldItem defines an item in a sale.
type SoldItem struct {
	ItemID  uuuid.UUID `bson:"itemID,omitempty" json:"itemID,omitempty"`
	Barcode string     `bson:"barcode,omitempty" json:"barcode,omitempty"`
	Weight  float64    `bson:"weight,omitempty" json:"weight,omitempty"`
	Lot     string     `bson:"lot,omitempty" json:"lot,omitempty"`
	SKU     string     `bson:"sku,omitempty" json:"sku,omitempty"`
}

// BSON#Unmarshal errors out when unmarshalling to map due to presence of array.
// Since we can't directly unmarshal to Sale, hence this. There has to be a better way.
type saleBSON struct {
	ID        objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	SaleID    string            `bson:"saleID,omitempty" json:"saleID,omitempty"`
	Items     []soldItemXSON    `bson:"items,omitempty" json:"items,omitempty"`
	Timestamp int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
}

// Same as saleBSON
type saleJSON struct {
	ID        string         `bson:"_id,omitempty" json:"_id,omitempty"`
	SaleID    string         `bson:"saleID,omitempty" json:"saleID,omitempty"`
	Items     []soldItemXSON `bson:"items,omitempty" json:"items,omitempty"`
	Timestamp int64          `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
}

type soldItemXSON struct {
	ItemID  string  `bson:"itemID,omitempty" json:"itemID,omitempty"`
	Barcode string  `bson:"barcode,omitempty" json:"barcode,omitempty"`
	Weight  float64 `bson:"weight,omitempty" json:"weight,omitempty"`
	Lot     string  `bson:"lot,omitempty" json:"lot,omitempty"`
	SKU     string  `bson:"sku,omitempty" json:"sku,omitempty"`
}

// MarshalBSON returns bytes of BSON-type.
func (s Sale) MarshalBSON() ([]byte, error) {
	items := make([]map[string]interface{}, 0)
	for _, item := range s.Items {
		items = append(items, map[string]interface{}{
			"itemID":  item.ItemID.String(),
			"barcode": item.Barcode,
			"weight":  item.Weight,
			"lot":     item.Lot,
			"sku":     item.SKU,
		})
	}

	in := map[string]interface{}{
		"timestamp": s.Timestamp,
	}
	if s.SaleID != (uuuid.UUID{}) {
		in["saleID"] = s.SaleID.String()
	}

	if s.ID != objectid.NilObjectID {
		in["_id"] = s.ID
	}
	if len(items) > 0 {
		in["items"] = items
	}

	return bson.Marshal(in)
}

// MarshalJSON returns bytes of JSON-type.
func (s *Sale) MarshalJSON() ([]byte, error) {
	items := make([]map[string]interface{}, 0)
	for _, item := range s.Items {
		items = append(items, map[string]interface{}{
			"itemID":  item.ItemID.String(),
			"barcode": item.Barcode,
			"weight":  item.Weight,
			"lot":     item.Lot,
			"sku":     item.SKU,
		})
	}

	in := map[string]interface{}{
		"timestamp": s.Timestamp,
	}

	if s.ID != objectid.NilObjectID {
		in["_id"] = s.ID.Hex()
	}
	if s.SaleID != (uuuid.UUID{}) {
		in["saleID"] = s.SaleID.String()
	}
	if len(items) > 0 {
		in["items"] = items
	}

	return json.Marshal(in)
}

// UnmarshalBSON returns BSON-type from bytes.
func (s *Sale) UnmarshalBSON(in []byte) error {
	sb := &saleBSON{}
	err := bson.Unmarshal(in, sb)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
		return err
	}

	s.Timestamp = sb.Timestamp

	if sb.ID != objectid.NilObjectID {
		s.ID = sb.ID
	}
	saleID, err := uuuid.FromString(sb.SaleID)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error: Error parsing SaleID")
	}
	s.SaleID = saleID

	if s.Items == nil {
		s.Items = make([]SoldItem, 0)
	}
	for _, item := range sb.Items {
		itemID, err := uuuid.FromString(item.ItemID)
		if err != nil {
			err = errors.Wrap(err, "UnmarshalBSON: Error parsing ItemID")
			return err
		}
		s.Items = append(s.Items, SoldItem{
			ItemID:  itemID,
			Barcode: item.Barcode,
			Weight:  item.Weight,
			Lot:     item.Lot,
			SKU:     item.SKU,
		})
	}
	return nil
}

// UnmarshalJSON returns JSON-type from bytes.
func (s *Sale) UnmarshalJSON(in []byte) error {
	sb := &saleJSON{}
	err := json.Unmarshal(in, sb)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalJSON Error")
		return err
	}

	s.Timestamp = sb.Timestamp

	if sb.ID != "" && sb.ID != objectid.NilObjectID.String() {
		s.ID, err = objectid.FromHex(sb.ID)
		if err != nil {
			err = errors.Wrap(err, "UnmarshalJSON Error: Error parsing ObjectID")
			return err
		}
	}
	s.SaleID, err = uuuid.FromString(sb.SaleID)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalJSON Error: Error parsing SaleID")
		return err
	}

	if s.Items == nil {
		s.Items = make([]SoldItem, 0)
	}
	for _, item := range sb.Items {
		itemID, err := uuuid.FromString(item.ItemID)
		if err != nil {
			err = errors.Wrap(err, "UnmarshalJSON: Error parsing ItemID")
			return err
		}
		s.Items = append(s.Items, SoldItem{
			ItemID:  itemID,
			Barcode: item.Barcode,
			Weight:  item.Weight,
			Lot:     item.Lot,
			SKU:     item.SKU,
		})
	}
	return nil
}
