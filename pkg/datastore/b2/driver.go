package b2

import (
	"context"
	"database/sql"
	"errors"

	blazer "github.com/kurin/blazer/b2"
	"github.com/takama/dbsync/pkg/datastore/file"
)

// B2 driver
type B2 struct {
	ctx context.Context
	*blazer.Client
	path    file.Fields
	header  file.Fields
	columns file.Fields
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for BackBlaze B2")

// New creates B2 driver
func New(accountID, appKey string, path, header file.Fields, columns ...file.Field) (db *B2, err error) {
	return nil, ErrUnsupported
}

// LastID implements interface for getting last ID in datastore bucket
func (db *B2) LastID(bucket string) (id uint64, err error) {
	return 0, ErrUnsupported
}

// GetByID is not implemented
func (db *B2) GetByID(table string, ID interface{}) (*sql.Row, error) {
	return nil, ErrUnsupported
}

// GetLimited is not implemented
func (db *B2) GetLimited(table string, limit uint64) (*sql.Rows, error) {
	return nil, ErrUnsupported
}

// GetLimitedAfterID is not implemented
func (db *B2) GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error) {
	return nil, ErrUnsupported
}

// Update is not implemented
func (db *B2) Update(table string, columns []string, values []interface{}) (count uint64, err error) {
	return 0, ErrUnsupported
}

// Insert implements interface for inserting data into bucket
func (db *B2) Insert(bucket string, columns []string, values []interface{}) (last uint64, err error) {
	return 0, ErrUnsupported
}
