package s3

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	s3svc "github.com/aws/aws-sdk-go/service/s3"
	"github.com/takama/dbsync/pkg/datastore/binding"
	"github.com/takama/dbsync/pkg/datastore/mapping"
)

// S3 driver
type S3 struct {
	ctx context.Context
	*s3svc.Bucket
	*s3svc.S3
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
var ErrUnsupported = errors.New("Unsupported method for Amazone S3")

// New creates S3 driver
func New(
	accountRegion, accountID, accountKey, accountToken, bucket, id string,
	json, compression bool, topics []string, extension string,
	exclude, spec, path, name, header, columns mapping.Fields,
) (db *S3, err error) {
	db = &S3{
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
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(accountRegion),
		Credentials: credentials.NewStaticCredentials(accountID, accountKey, accountToken),
	})
	if err != nil {
		return
	}
	db.S3 = s3svc.New(s)
	if bucket != "" {
		err = db.checkBucket(bucket)
	}
	return
}

// LastID implements interface for getting last ID in datastore bucket
func (db *S3) LastID(bucket string) (id uint64, err error) {
	bkt, err := db.getBucket(bucket)
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
func (db *S3) AddFromSQL(bucket string, columns []string, values []interface{}) (last uint64, err error) {
	return 0, ErrUnsupported
}

// Close flushes data and closes files
func (db *S3) Close() (err error) {
	return
}

// GetFiles should collect
func (db *S3) GetFiles(path string, fileCount int) (collection map[string]binding.Stream, err error) {
	err = ErrUnsupported
	return
}

// PutFile uploads file to the datastore
func (db *S3) PutFile(path string, stream binding.Stream) error {
	if stream.Handle != nil {
		defer stream.Handle.Close()
		// Save data
		if str := strings.Trim(db.extension, ". "); str != "" {
			path = path + "." + str
		}
		_, err := db.S3.PutObject(&s3svc.PutObjectInput{
			Body:   stream.Handle,
			Bucket: aws.String(db.bucket),
			Key:    aws.String(path),
		})
		return err
	}

	return nil
}

// Remove method removes file by path
func (db *S3) Remove(path string) error {
	return ErrUnsupported
}

func (db *S3) getOrCreateLastID(bucket string) (id uint64, err error) {
	_, err = db.getBucket(bucket)
	if err != nil {
		return
	}

	return
}

func (db *S3) checkBucket(bucket string) (err error) {
	result, err := db.S3.ListBuckets(nil)
	if err != nil {
		return
	}

	for _, b := range result.Buckets {
		if aws.StringValue(b.Name) == bucket {
			return nil
		}
	}
	_, err = db.S3.CreateBucket(&s3svc.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return
	}
	return db.S3.WaitUntilBucketExists(&s3svc.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
}

func (db *S3) getBucket(name string) (bucket string, err error) {
	if db.bucket != "" {
		return db.bucket, nil
	}
	r := strings.NewReplacer("-", "", "_", "", " ", "")
	bucket = r.Replace(name)
	err = db.checkBucket(bucket)
	return
}
