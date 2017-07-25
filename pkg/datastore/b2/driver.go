package b2

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"

	blazer "github.com/kurin/blazer/b2"
	"github.com/takama/dbsync/pkg/datastore/binding"
)

// B2 driver
type B2 struct {
	ctx context.Context
	*blazer.Client
	*blazer.Bucket

	compression bool
	extension   string
	bucket      string
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for BackBlaze B2")

// New creates B2 driver
func New(
	accountID, accountKey string,
	compression bool, extension, bucket string,
) (db *B2, err error) {
	db = &B2{
		ctx: context.Background(),

		compression: compression,
		extension:   extension,
		bucket:      bucket,
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

// Close flushes data and closes files
func (db *B2) Close() (err error) {
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
