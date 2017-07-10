package b2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	blazer "github.com/kurin/blazer/b2"
	"github.com/takama/dbsync/pkg/datastore/mapping"
)

// B2 driver
type B2 struct {
	ctx context.Context
	*blazer.Client
	json    bool
	id      string
	topics  []string
	spec    mapping.Fields
	path    mapping.Fields
	name    mapping.Fields
	header  mapping.Fields
	columns mapping.Fields
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for BackBlaze B2")

// New creates B2 driver
func New(
	accountID, appKey, id string, json bool, topics []string,
	spec, path, name, header, columns mapping.Fields,
) (db *B2, err error) {
	db = &B2{
		ctx:     context.Background(),
		json:    json,
		id:      id,
		topics:  topics,
		spec:    spec,
		path:    path,
		name:    name,
		header:  header,
		columns: columns,
	}
	client, err := blazer.NewClient(db.ctx, accountID, appKey)
	if err != nil {
		return
	}
	db.Client = client
	return
}

// LastID implements interface for getting last ID in datastore bucket
func (db *B2) LastID(bucket string) (id uint64, err error) {
	bkt, err := db.getOrCreateBucket(db.bucketName(bucket))
	if err != nil {
		return
	}
	id, err = db.getOrCreateLastID(bkt)
	if err != nil {
		return
	}
	return id, nil
}

// AddFromSQL implements interface for inserting data from SQL into bucket
func (db *B2) AddFromSQL(bucket string, columns []string, values []interface{}) (last uint64, err error) {
	bkt, err := db.getOrCreateBucket(db.bucketName(bucket))
	if err != nil {
		return
	}

	// Get last ID
	lastID := "0"
	for ndx, name := range columns {
		if name == db.id {
			if value, ok := values[ndx].([]byte); ok {
				lastID = string(value)
				last, err = strconv.ParseUint(lastID, 10, 64)
				if err != nil {
					return
				}
			}
			break
		}
	}
	for _, topic := range db.topics {

		// Generate path
		path := topic
		for _, field := range db.path {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + mapping.Render(field, "/", "", false, false, columns, values)
		}

		// Generate name
		for _, field := range db.name {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + mapping.Render(field, "/", "", false, false, columns, values)
		}

		// Generate header
		data := "\n"
		for _, field := range db.header {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			data = data + mapping.Render(field, " ", "", false, false, columns, values)
		}
		data = data + "\n" + strings.Repeat("=", len(data)) + "\n"

		// Generate data columns
		if len(db.columns) > 0 {
			if db.json {
				data = data + "{"
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + mapping.Render(field, ": ", ", ", true, true, columns, values)
				}
				data = strings.Trim(data, ", ") + "}"
			} else {
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + mapping.Render(field, ": ", "\n", true, false, columns, values)
				}
			}
		} else {
			data = data + mapping.Render(
				mapping.Field{Type: "string", Format: "%s"},
				": ", "\n", true, false, columns, values,
			)
		}

		// Save data
		datafile := bkt.Object(path)
		wf := datafile.NewWriter(db.ctx)
		wf.ChunkSize = 5e6
		defer wf.Close()
		if _, err := io.Copy(wf, strings.NewReader(data)); err != nil {
			return 0, err
		}
	}

	// Save Last ID
	w := bkt.Object("lastID").NewWriter(db.ctx)
	defer w.Close()
	if _, err := io.Copy(w, strings.NewReader(lastID)); err != nil {
		return 0, err
	}
	return
}

// Close flushes data and closes files
func (db *B2) Close() (err error) {
	return
}

func (db *B2) getOrCreateBucket(bucket string) (bkt *blazer.Bucket, err error) {
	buckets, err := db.Client.ListBuckets(db.ctx)
	if err != nil {
		return
	}
	for _, b := range buckets {
		if b.Name() == bucket {
			return b, nil
		}
	}
	bkt, err = db.Client.NewBucket(db.ctx, bucket, nil)
	return
}

func (db *B2) getOrCreateLastID(bkt *blazer.Bucket) (id uint64, err error) {
	lastID := bkt.Object("lastID")
	_, err = lastID.Attrs(db.ctx)
	if err != nil {
		w := lastID.NewWriter(db.ctx)
		defer w.Close()
		if _, err := io.Copy(w, strings.NewReader("0")); err != nil {
			return 0, err
		}
		return 0, nil
	}
	r := lastID.NewReader(db.ctx)
	r.ChunkSize = 32
	defer r.Close()
	buffer := new(bytes.Buffer)
	if _, err := io.Copy(buffer, r); err != nil {
		return 0, err
	}
	if err := r.Close(); err != nil {
		return 0, err
	}
	id, err = strconv.ParseUint(buffer.String(), 10, 64)
	if err != nil {
		return
	}
	return
}

func (db *B2) bucketName(bucket string) string {
	r := strings.NewReplacer("-", "", "_", "", " ", "")
	return r.Replace(bucket)
}
