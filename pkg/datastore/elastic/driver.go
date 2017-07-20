package elastic

import (
	"context"
	"errors"

	"fmt"

	es "gopkg.in/olivere/elastic.v5"
)

// Elastic driver
type Elastic struct {
	ctx context.Context
	*es.Client
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for ElasticSearch v5")

// New creates Elastic driver
func New(host string, port int) (db *Elastic, err error) {
	db = &Elastic{
		ctx: context.Background(),
	}
	db.Client, err = es.NewClient(es.SetURL(fmt.Sprintf("http://%s:%d", host, port)))
	return
}

// AddFromSQL implements interface for inserting data from SQL into ElasticSearch
func (db *Elastic) AddFromSQL(document string, columns []string, values []interface{}) (err error) {
	return ErrUnsupported
}

// Close flushes data
func (db *Elastic) Close() (err error) {
	return
}
