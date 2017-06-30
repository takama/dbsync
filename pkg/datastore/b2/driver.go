package b2

import (
	"database/sql"
	"errors"
)

// B2 driver
type B2 struct {
}

// New creates B2 driver
func New(host string, port uint64, database, username, password string) (db *B2, err error) {
	return nil, errors.New("Unsupported method")
}

// LastID implements interface for getting last ID in database table
func (db *B2) LastID(table string) (id uint64, err error) {
	return 0, errors.New("Unsupported method")
}

// GetByID implements interface for getting table row by ID
func (db *B2) GetByID(table string, ID interface{}) (*sql.Row, error) {
	return nil, errors.New("Unsupported method")
}

// GetLimited implements interface for getting last limited table rows by ID
func (db *B2) GetLimited(table string, limit uint64) (*sql.Rows, error) {
	return nil, errors.New("Unsupported method")
}

// GetLimitedAfterID implements interface for getting limited table rows after specified ID
func (db *B2) GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error) {
	return nil, errors.New("Unsupported method")
}

// Update implements interface for updating table rows
func (db *B2) Update(table string, columns []string, values []interface{}) (count uint64, err error) {
	return 0, errors.New("Unsupported method")
}

// Insert implements interface for inserting table rows
func (db *B2) Insert(table string, columns []string, values []interface{}) (last uint64, err error) {
	return 0, errors.New("Unsupported method")
}
