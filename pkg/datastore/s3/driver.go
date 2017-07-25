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
)

// S3 driver
type S3 struct {
	ctx context.Context
	*s3svc.Bucket
	*s3svc.S3

	extension string
	bucket    string
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for Amazone S3")

// New creates S3 driver
func New(
	accountRegion, accountID, accountKey, accountToken, extension, bucket string,
) (db *S3, err error) {
	db = &S3{
		ctx:       context.Background(),
		extension: extension,
		bucket:    bucket,
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

// Close flushes data and closes files
func (db *S3) Close() (err error) {
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
