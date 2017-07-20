package b2

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	blazer "github.com/kurin/blazer/b2"
	"github.com/takama/dbsync/pkg/datastore/binding"
	"github.com/takama/dbsync/pkg/datastore/mapping"
)

// B2 driver
type B2 struct {
	ctx context.Context
	*blazer.Client
	*blazer.Bucket
	json        bool
	compression bool
	extension   string
	bucket      string
	id          string
	topics      []string
	exclude     mapping.Fields
	spec        mapping.Fields
	path        mapping.Fields
	name        mapping.Fields
	header      mapping.Fields
	columns     mapping.Fields
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for BackBlaze B2")

// New creates B2 driver
func New(
	accountID, accountKey, bucket, id string, json, compression bool, topics []string,
	extension string, exclude, spec, path, name, header, columns mapping.Fields,
) (db *B2, err error) {
	db = &B2{
		ctx:         context.Background(),
		json:        json,
		compression: compression,
		extension:   extension,
		bucket:      bucket,
		id:          id,
		topics:      topics,
		exclude:     exclude,
		spec:        spec,
		path:        path,
		name:        name,
		header:      header,
		columns:     columns,
	}
	client, err := blazer.NewClient(db.ctx, accountID, accountKey)
	if err != nil {
		return
	}
	db.Client = client
	if bucket != "" {
		bkt, err := db.getOrCreateBucket(bucket)
		if err != nil {
			return nil, err
		}
		db.Bucket = bkt
	}
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
			path = path + mapping.RenderTxt(field, "/", "", false, false, columns, values)
		}

		// Generate name
		for _, field := range db.name {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + mapping.RenderTxt(field, "/", "", false, false, columns, values)
		}
		if str := strings.Trim(db.extension, ". "); str != "" {
			path = path + "." + str
		}

		// Generate header
		data := "\n"
		for _, field := range db.header {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			data = data + mapping.RenderTxt(field, " ", "", false, false, columns, values)
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
					data = data + mapping.RenderTxt(field, ": ", ", ", true, true, columns, values)
				}
				data = strings.Trim(data, ", ") + "}"
			} else {
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + mapping.RenderTxt(field, ": ", "\n", true, false, columns, values)
				}
			}
		} else {
			data = data + mapping.RenderTxt(
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

// GetFiles should collect
func (db *B2) GetFiles(path string, fileCount int) (collection map[string]binding.Stream, err error) {
	err = ErrUnsupported
	return
}

// PutFile uploads file to the datastore
func (db *B2) PutFile(path string, stream binding.Stream) error {
	if stream.Handle != nil {
		defer stream.Handle.Close()
		// Save data
		if str := strings.Trim(db.extension, ". "); str != "" {
			path = path + "." + str
		}
		datafile := db.Bucket.Object(path)
		w := datafile.NewWriter(db.ctx)
		w.ConcurrentUploads = 5

		if db.compression {
			gz := gzip.NewWriter(w)
			if _, err := io.Copy(gz, stream.Reader); err != nil {
				w.Close()
				return err
			}
			if err := gz.Flush(); err != nil {
				w.Close()
				return err
			}
		} else {
			if _, err := io.Copy(w, stream.Reader); err != nil {
				w.Close()
				return err
			}
		}
		return w.Close()
	}

	return nil
}

// Remove method removes file by path
func (db *B2) Remove(path string) error {
	return ErrUnsupported
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
	if db.bucket != "" {
		return db.bucket
	}
	r := strings.NewReplacer("-", "", "_", "", " ", "")
	return r.Replace(bucket)
}
