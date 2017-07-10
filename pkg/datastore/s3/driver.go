package s3

import (
	"database/sql"
	"errors"
)

// S3 driver
type S3 struct {
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for Amazone S3")

// New creates S3 driver
func New(host string, port uint64, database, username, password string) (db *S3, err error) {
	return nil, ErrUnsupported
}

// LastID implements interface for getting last ID in database table
func (db *S3) LastID(table string) (id uint64, err error) {
	return 0, ErrUnsupported
}

// GetByID implements interface for getting table row by ID
func (db *S3) GetByID(table string, ID interface{}) (*sql.Row, error) {
	return nil, ErrUnsupported
}

// GetLimited implements interface for getting last limited table rows by ID
func (db *S3) GetLimited(table string, limit uint64) (*sql.Rows, error) {
	return nil, ErrUnsupported
}

// GetLimitedAfterID implements interface for getting limited table rows after specified ID
func (db *S3) GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error) {
	return nil, ErrUnsupported
}

// Update implements interface for updating table rows
func (db *S3) Update(table string, columns []string, values []interface{}) (count uint64, err error) {
	return 0, ErrUnsupported
}

// Insert implements interface for inserting table rows
func (db *S3) Insert(table string, columns []string, values []interface{}) (last uint64, err error) {
	return 0, ErrUnsupported
}

// Close flushes data and closes files
func (db *S3) Close() (err error) {
	return
}
