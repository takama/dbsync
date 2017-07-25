package elastic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/takama/dbsync/pkg/datastore/mapping"
	es "gopkg.in/olivere/elastic.v5"
)

// Elastic driver
type Elastic struct {
	ctx context.Context
	*es.BulkProcessor

	renderMap *mapping.RenderMap

	include mapping.Fields
	exclude mapping.Fields
	columns mapping.Fields
	extras  mapping.Fields
	spec    mapping.Fields
	indices mapping.Fields
	cursors map[string]*mapping.Cursor
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for ElasticSearch v5")

// ErrNotAcknowledged declares error for not acknowledged index
var ErrNotAcknowledged = errors.New("Create index not acknowledged")

// ErrEmptyID declares error for invalid ID
var ErrEmptyID = errors.New("Invalid ID data")

// New creates Elastic driver
func New(
	host string, port uint64, renderMap *mapping.RenderMap,
	include, exclude, columns, extras, spec, indices mapping.Fields,
) (db *Elastic, err error) {
	db = &Elastic{
		ctx: context.Background(),

		renderMap: renderMap,

		include: include,
		exclude: exclude,
		columns: columns,
		extras:  extras,

		spec:    spec,
		indices: indices,
		cursors: make(map[string]*mapping.Cursor),
	}
	client, err := es.NewClient(es.SetURL(fmt.Sprintf("http://%s:%d", host, port)))
	if err != nil {
		return
	}
	db.BulkProcessor, err = client.BulkProcessor().Name("migration").Workers(2).Do(db.ctx)
	return
}

// LastID implements interface for getting last ID in datastore document
func (db *Elastic) LastID(document string) (id uint64, err error) {
	cursor := db.getCursor(document)
	v, err := strconv.ParseUint(string(cursor.ID), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Cursor sets pointer
func (db *Elastic) Cursor(
	document string, renderMap *mapping.RenderMap, columns []string, values []interface{},
) (last uint64, err error) {
	// Get and decode cursor
	cursor := db.getCursor(document)
	cursor.Decode(db.spec, renderMap, columns, values)

	// Save cursor
	db.cursors[document] = cursor
	last, err = strconv.ParseUint(cursor.ID, 10, 64)
	if err != nil {
		return
	}
	if last == 0 {
		return last, ErrEmptyID
	}

	return
}

// AddFromSQL implements interface for inserting data from SQL into ElasticSearch
func (db *Elastic) AddFromSQL(document string, columns []string, values []interface{}) (last uint64, err error) {

	last, err = db.Cursor(document, db.renderMap, columns, values)
	if err != nil {
		return
	}
	jsonMap := new(mapping.RenderMap)
	*jsonMap = *db.renderMap
	jsonMap.Delimiter = ": "
	jsonMap.Finalizer = ", "
	jsonMap.UseNames = true
	jsonMap.Quotas = true

	// Check index
	for _, field := range db.indices {
		index := db.renderMap.Render(field, columns, values)

		// Write data
		if len(db.columns) > 0 {
			data := "{"
			jsonMap.Extras = false
			for _, field := range db.columns {
				data = data + jsonMap.Render(field, columns, values)
			}
			jsonMap.Extras = true
			for _, field := range db.extras {
				data = data + jsonMap.Render(field, columns, values)
			}
			data = strings.Trim(data, ", ") + "}"
			request := es.NewBulkIndexRequest().Index(index).Type(document).Doc(data)
			db.Add(request)
		}
	}

	return
}

// Close flushes data
func (db *Elastic) Close() (err error) {
	return db.Flush()
}

func (db *Elastic) getCursor(document string) (cursor *mapping.Cursor) {
	cursor, ok := db.cursors[document]
	if ok {
		return
	}
	cursor = &mapping.Cursor{ID: "0", AT: "0000-00-00"}
	db.cursors[document] = cursor

	return
}
