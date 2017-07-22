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
	*es.Client
	id      string
	spec    mapping.Fields
	indices mapping.Fields
	include mapping.Fields
	exclude mapping.Fields
	columns mapping.Fields
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
	host string, port uint64, id string,
	spec, indices, include, exclude, columns mapping.Fields,
) (db *Elastic, err error) {
	db = &Elastic{
		ctx:     context.Background(),
		id:      id,
		spec:    spec,
		indices: indices,
		include: include,
		exclude: exclude,
		columns: columns,
		cursors: make(map[string]*mapping.Cursor),
	}
	db.Client, err = es.NewClient(es.SetURL(fmt.Sprintf("http://%s:%d", host, port)))
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

// AddFromSQL implements interface for inserting data from SQL into ElasticSearch
func (db *Elastic) AddFromSQL(document string, columns []string, values []interface{}) (last uint64, err error) {
	// Get and decode cursor
	cursor := db.getCursor(document)
	cursor.Decode(db.spec, columns, values)

	// Save cursor
	db.cursors[document] = cursor
	last, err = strconv.ParseUint(cursor.ID, 10, 64)
	if err != nil {
		return
	}
	if last == 0 {
		return last, ErrEmptyID
	}

	// Check filters
	if len(db.include) == 0 {
		for _, field := range db.exclude {
			if field.Topic == mapping.RenderTxt(field, "", "", false, false, columns, values) {
				return
			}
		}
	} else {
		found := false
		for _, field := range db.include {
			if field.Topic == mapping.RenderTxt(field, "", "", false, false, columns, values) {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}

	// Check index
	for _, field := range db.indices {
		index := mapping.RenderTxt(field, "", "", false, false, columns, values)
		exist, err := db.IndexExists(index).Do(db.ctx)
		if err != nil {
			return last, err
		}
		// Create index and mapping if they are not exist
		if !exist {
			createIndex, err := db.CreateIndex(index).Do(db.ctx)
			if err != nil {
				return last, err
			}
			if !createIndex.Acknowledged {
				return last, ErrNotAcknowledged
			}
		}

		// Write data
		if len(db.columns) > 0 {
			data := "{"
			for _, field := range db.columns {
				data = data + mapping.RenderTxt(field, ": ", ", ", true, true, columns, values)
			}
			data = strings.Trim(data, ", ") + "}"
			_, err = db.Index().
				Index(index).
				Type(document).
				BodyString(data).
				Do(db.ctx)
			if err != nil {
				return last, err
			}
			_, err = db.Flush().Index(index).Do(db.ctx)
			if err != nil {
				return last, err
			}
		}
	}

	return
}

// Close flushes data
func (db *Elastic) Close() (err error) {
	return
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
