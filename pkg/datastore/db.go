package datastore

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/takama/dbsync/pkg/datastore/b2"
	"github.com/takama/dbsync/pkg/datastore/binding"
	"github.com/takama/dbsync/pkg/datastore/elastic"
	"github.com/takama/dbsync/pkg/datastore/file"
	"github.com/takama/dbsync/pkg/datastore/mapping"
	"github.com/takama/dbsync/pkg/datastore/mysql"
	"github.com/takama/dbsync/pkg/datastore/postgres"
	"github.com/takama/dbsync/pkg/datastore/s3"
	"github.com/takama/envconfig"
)

type syncType int

// synchronization types
const (
	sql2sql syncType = iota
	sql2file
	sql2doc
	file2file
)

// DBHandler provide a simple handler interface for DB
type DBHandler interface {
	Run() error
	Report() []Status
	Shutdown() error
}

// Status contains detailed sync information
type Status struct {
	Table    string
	Inserted uint64
	Updated  uint64
	Errors   uint64
	LastID   uint64
	Running  bool
	Changed  time.Time
	Duration string
}

type sqlReplication interface {
	LastID(table string) (uint64, error)
	GetByID(table string, ID interface{}) (*sql.Row, error)
	GetLimited(table string, limit uint64) (*sql.Rows, error)
	GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error)
	Update(table string, columns []string, values []interface{}) (count uint64, err error)
	Insert(table string, columns []string, values []interface{}) (lastID uint64, err error)
	Close() error
}

type documentReplication interface {
	LastID(document string) (uint64, error)
	AddFromSQL(document string, columns []string, values []interface{}) (lastID uint64, err error)
	Close() error
}

type fileReplication interface {
	LastID(bucket string) (uint64, error)
	AddFromSQL(bucket string, columns []string, values []interface{}) (lastID uint64, err error)
	GetFiles(path string, fileCount int) (collection map[string]binding.Stream, err error)
	PutFile(path string, stream binding.Stream) error
	Remove(path string) error
	Close() error
}

// DBBundle contains drivers/documents/tables information
type DBBundle struct {
	mutex     sync.RWMutex
	done      chan bool
	wg        *sync.WaitGroup
	stdlog    *log.Logger
	errlog    *log.Logger
	status    []Status
	direction syncType

	// SQL drivers interfaces
	srcSQLDriver sqlReplication
	dstSQLDriver sqlReplication

	// Document drivers interfaces
	srcDocumentDriver documentReplication
	dstDocumentDriver documentReplication

	// File drivers interfaces
	srcFileDriver fileReplication
	dstFileDriver fileReplication

	// Documents/Tables names declaration
	UpdateDocuments []string `split_words:"true"`
	InsertDocuments []string `split_words:"true"`
	DocumentsPrefix string   `split_words:"true"`
	DocumentsSuffix string   `split_words:"true"`

	// Cursor synchronization specification
	CursorSpec mapping.Fields `split_words:"true"`

	// ID pointer management
	IDName       string `envconfig:"DBSYNC_ID_NAME"`
	StartAfterID uint64 `split_words:"true"`
	StopBeforeID uint64 `split_words:"true"`
	Reverse      bool   `split_words:"true"`

	// Periods in seconds between bulk operations
	UpdatePeriod uint64 `split_words:"true" required:"true"`
	InsertPeriod uint64 `split_words:"true" required:"true"`

	// Count of records for operations
	UpdateRecords uint64 `split_words:"true" required:"true"`
	InsertRecords uint64 `split_words:"true" required:"true"`

	// Count of documents for operations
	DocumentsSyncCount int `split_words:"true"`

	// File data directory
	FileDataDir string `split_words:"true"`

	// Source driver type
	SrcDriver string `split_words:"true" required:"true"`

	// Database replication source environments
	SrcDbHost     string `split_words:"true"`
	SrcDbPort     uint64 `split_words:"true"`
	SrcDbName     string `split_words:"true"`
	SrcDbUsername string `split_words:"true"`
	SrcDbPassword string `split_words:"true"`

	// Datastore account source environments (like cloud datastore S3)
	SrcAccountRegion string `split_words:"true"`
	SrcAccountID     string `split_words:"true"`
	SrcAccountKey    string `split_words:"true"`
	SrcAccountToken  string `split_words:"true"`

	// File replication source environments
	SrcFileJSON        bool           `split_words:"true"`
	SrcFileCompression bool           `split_words:"true"`
	SrcFileRemove      bool           `split_words:"true"`
	SrcFileExtension   string         `split_words:"true"`
	SrcFileBucket      string         `split_words:"true"`
	SrcFileTopics      []string       `split_words:"true"`
	SrcFileMatch       string         `split_words:"true"`
	SrcFileExclude     mapping.Fields `split_words:"true"`
	SrcFileSpec        mapping.Fields `split_words:"true"`
	SrcFilePath        mapping.Fields `split_words:"true"`
	SrcFileName        mapping.Fields `split_words:"true"`
	SrcFileHeader      mapping.Fields `split_words:"true"`
	SrcFileColumns     mapping.Fields `split_words:"true"`

	// Destination driver type
	DstDriver string `split_words:"true" required:"true"`

	// Database replication destination environments
	DstDbHost     string `split_words:"true"`
	DstDbPort     uint64 `split_words:"true"`
	DstDbName     string `split_words:"true"`
	DstDbUsername string `split_words:"true"`
	DstDbPassword string `split_words:"true"`

	// Datastore account destination environments (like cloud datastore S3)
	DstAccountRegion string `split_words:"true"`
	DstAccountID     string `split_words:"true"`
	DstAccountKey    string `split_words:"true"`
	DstAccountToken  string `split_words:"true"`

	// File replication destination environments
	DstFileJSON        bool           `split_words:"true"`
	DstFileCompression bool           `split_words:"true"`
	DstFileExtension   string         `split_words:"true"`
	DstFileBucket      string         `split_words:"true"`
	DstFileTopics      []string       `split_words:"true"`
	DstFileMatch       string         `split_words:"true"`
	DstFileExclude     mapping.Fields `split_words:"true"`
	DstFileSpec        mapping.Fields `split_words:"true"`
	DstFilePath        mapping.Fields `split_words:"true"`
	DstFileName        mapping.Fields `split_words:"true"`
	DstFileHeader      mapping.Fields `split_words:"true"`
	DstFileColumns     mapping.Fields `split_words:"true"`

	// Documents replication destination environments
	DstDocIndices mapping.Fields `split_words:"true"`
	DstDocInclude mapping.Fields `split_words:"true"`
	DstDocExclude mapping.Fields `split_words:"true"`
	DstDocColumns mapping.Fields `split_words:"true"`
}

// ErrUnsupportedFileToSQL declares error for unsupported methods
var ErrUnsupportedFileToSQL = errors.New("Unsupported synchronization from file to SQL data")

// ErrUnsupportedFileToDocument declares error for unsupported methods
var ErrUnsupportedFileToDocument = errors.New("Unsupported synchronization from file to Document")

// ErrUnsupportedDocumentToSQL declares error for unsupported methods
var ErrUnsupportedDocumentToSQL = errors.New("Unsupported synchronization from document to SQL data")

// ErrUnsupportedDocumentToFile declares error for unsupported methods
var ErrUnsupportedDocumentToFile = errors.New("Unsupported synchronization from document to file")

// ErrUnsupportedDocumentToDocument declares error for unsupported methods
var ErrUnsupportedDocumentToDocument = errors.New("Unsupported synchronization from document to document")

// ErrNothingToSyncSource declares error if unspecified source driver
var ErrNothingToSyncSource = errors.New("Nothing to sync, please specify source driver")

// ErrNothingToSyncDestination declares error if unspecified destination driver
var ErrNothingToSyncDestination = errors.New("Nothing to sync, please specify destination driver")

// New creates new server
func New() (*DBBundle, error) {

	bundle := &DBBundle{
		done:   make(chan bool),
		wg:     &sync.WaitGroup{},
		stdlog: log.New(os.Stdout, "[DBSYNC:INFO]: ", log.LstdFlags),
		errlog: log.New(os.Stderr, "[DBSYNC:ERROR]: ", log.LstdFlags),
	}
	err := envconfig.Process("dbsync", bundle)
	if err != nil {
		return nil, err
	}

	for _, table := range bundle.UpdateDocuments {
		if !bundle.exists(table) {
			bundle.status = append(bundle.status, Status{Table: table})
		}
	}
	for _, table := range bundle.InsertDocuments {
		if !bundle.exists(table) {
			bundle.status = append(bundle.status, Status{Table: table})
		}
	}

	for _, topic := range bundle.SrcFileTopics {
		if !bundle.exists(topic) {
			bundle.status = append(bundle.status, Status{Table: topic})
		}
	}

	switch strings.ToLower(bundle.SrcDriver) {
	case "elastic":
		return nil, elastic.ErrUnsupported
	case "b2":
		return nil, b2.ErrUnsupported
	case "s3":
		return nil, s3.ErrUnsupported
	case "pgsql":
		bundle.srcSQLDriver, err = postgres.New(
			bundle.SrcDbHost, bundle.SrcDbPort, bundle.SrcDbName,
			bundle.SrcDbUsername, bundle.SrcDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "mysql":
		bundle.srcSQLDriver, err = mysql.New(
			bundle.SrcDbHost, bundle.SrcDbPort, bundle.SrcDbName,
			bundle.SrcDbUsername, bundle.SrcDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "file":
		bundle.srcFileDriver, err = file.New(
			bundle.FileDataDir, bundle.SrcFileBucket, bundle.IDName,
			bundle.SrcFileJSON, bundle.SrcFileCompression, bundle.SrcFileTopics,
			bundle.SrcFileExtension, bundle.SrcFileMatch, bundle.SrcFileExclude,
			bundle.SrcFileSpec, bundle.SrcFilePath, bundle.SrcFileName,
			bundle.SrcFileHeader, bundle.SrcFileColumns,
		)
		if err != nil {
			return bundle, err
		}
	}
	switch strings.ToLower(bundle.DstDriver) {
	case "elastic":
		bundle.dstDocumentDriver, err = elastic.New(
			bundle.DstDbHost, bundle.DstDbPort, bundle.IDName, bundle.CursorSpec,
			bundle.DstDocIndices, bundle.DstDocInclude, bundle.DstDocExclude, bundle.DstDocColumns,
		)
	case "b2":
		bundle.dstFileDriver, err = b2.New(
			bundle.DstAccountID, bundle.DstAccountKey, bundle.DstFileBucket, bundle.IDName,
			bundle.DstFileJSON, bundle.DstFileCompression, bundle.DstFileTopics,
			bundle.DstFileExtension, bundle.DstFileExclude, bundle.DstFileSpec, bundle.DstFilePath,
			bundle.DstFileName, bundle.DstFileHeader, bundle.DstFileColumns,
		)
		if err != nil {
			return bundle, err
		}
	case "s3":
		bundle.dstFileDriver, err = s3.New(
			bundle.DstAccountRegion, bundle.DstAccountID, bundle.DstAccountKey,
			bundle.DstAccountToken, bundle.DstFileBucket, bundle.IDName,
			bundle.DstFileJSON, bundle.DstFileCompression, bundle.DstFileTopics,
			bundle.DstFileExtension, bundle.DstFileExclude, bundle.DstFileSpec, bundle.DstFilePath,
			bundle.DstFileName, bundle.DstFileHeader, bundle.DstFileColumns,
		)
		if err != nil {
			return bundle, err
		}
	case "pgsql":
		bundle.dstSQLDriver, err = postgres.New(
			bundle.DstDbHost, bundle.DstDbPort, bundle.DstDbName,
			bundle.DstDbUsername, bundle.DstDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "mysql":
		bundle.dstSQLDriver, err = mysql.New(
			bundle.DstDbHost, bundle.DstDbPort, bundle.DstDbName,
			bundle.DstDbUsername, bundle.DstDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "file":
		bundle.dstFileDriver, err = file.New(
			bundle.FileDataDir, bundle.DstFileBucket, bundle.IDName,
			bundle.DstFileJSON, bundle.DstFileCompression, bundle.DstFileTopics,
			bundle.DstFileExtension, bundle.DstFileMatch, bundle.DstFileExclude,
			bundle.DstFileSpec, bundle.DstFilePath, bundle.DstFileName,
			bundle.DstFileHeader, bundle.DstFileColumns,
		)
		if err != nil {
			return bundle, err
		}
	}

	return bundle, err
}

// Run implements interface that starts synchronization of the db items
func (dbb *DBBundle) Run() error {
	if dbb.srcFileDriver == nil && dbb.srcSQLDriver == nil && dbb.srcDocumentDriver == nil {
		// nothing to convert
		return ErrNothingToSyncSource
	}
	if dbb.dstFileDriver == nil && dbb.dstSQLDriver == nil && dbb.dstDocumentDriver == nil {
		// nothing to convert
		return ErrNothingToSyncDestination
	}
	if dbb.srcFileDriver != nil && dbb.dstSQLDriver != nil {
		// unsupported (File to SQL)
		return ErrUnsupportedFileToSQL
	}
	if dbb.srcFileDriver != nil && dbb.dstDocumentDriver != nil {
		// unsupported (File to Document)
		return ErrUnsupportedFileToDocument
	}
	if dbb.srcDocumentDriver != nil && dbb.dstSQLDriver != nil {
		// unsupported (Document to SQL)
		return ErrUnsupportedDocumentToSQL
	}
	if dbb.srcDocumentDriver != nil && dbb.dstFileDriver != nil {
		// unsupported (Document to File)
		return ErrUnsupportedDocumentToFile
	}
	if dbb.srcDocumentDriver != nil && dbb.dstDocumentDriver != nil {
		// unsupported (Document to Document)
		return ErrUnsupportedDocumentToDocument
	}
	if dbb.srcFileDriver != nil {
		if dbb.dstFileDriver != nil {
			dbb.direction = file2file
		}
	} else {
		if dbb.dstFileDriver != nil {
			dbb.direction = sql2file
		} else {
			if dbb.dstDocumentDriver != nil {
				dbb.direction = sql2doc
			} else {
				dbb.direction = sql2sql
			}
		}
	}
	go func() {
		// usecases:
		// 1. File to File/B2/S3
		// 2. SQL to Document
		// 3. SQL to File/B2/S3
		// 4. SQL to SQL
		switch dbb.direction {
		case file2file:
			dbb.syncFileToFileHandler()
		case sql2file:
			fallthrough
		case sql2doc:
			dbb.fetchSQLHandler()
		case sql2sql:
			dbb.updateSQLToSQLHandler()
			dbb.fetchSQLHandler()
		}
		// setup handlers
		if dbb.srcSQLDriver != nil && dbb.dstSQLDriver != nil {
			// SQL to SQL
			updateTicker := time.NewTicker(time.Duration(dbb.UpdatePeriod) * time.Second)
			go func() {
				for range updateTicker.C {
					dbb.updateSQLToSQLHandler()
				}
			}()
		}
		insertTicker := time.NewTicker(time.Duration(dbb.InsertPeriod) * time.Second)
		go func() {
			for range insertTicker.C {
				switch dbb.direction {
				case file2file:
					dbb.syncFileToFileHandler()
				case sql2file:
					fallthrough
				case sql2doc:
					dbb.fetchSQLHandler()
				case sql2sql:
					dbb.updateSQLToSQLHandler()
					dbb.fetchSQLHandler()
				}
			}
		}()
	}()

	return nil
}

// Report implements interface that shows status detailed information
func (dbb *DBBundle) Report() []Status {
	dbb.mutex.RLock()
	var status []Status
	status = append(status, dbb.status...)
	dbb.mutex.RUnlock()
	for key, value := range status {
		status[key].Duration = time.Now().Sub(value.Changed).String()
	}
	return status
}

// Shutdown implements interface that makes graceful shutdown
func (dbb *DBBundle) Shutdown() error {
	close(dbb.done)
	dbb.wg.Wait()
	if dbb.dstFileDriver != nil {
		err := dbb.dstFileDriver.Close()
		if err != nil {
			return err
		}
	}
	if dbb.dstSQLDriver != nil {
		err := dbb.dstSQLDriver.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbb *DBBundle) exists(table string) bool {
	alreadyExists := false
	for _, status := range dbb.status {
		if status.Table == table {
			alreadyExists = true
			break
		}
	}
	return alreadyExists
}

func (dbb *DBBundle) updateSQLToSQLHandler() {
	dbb.wg.Add(1)
	defer dbb.wg.Done()
	for _, table := range dbb.UpdateDocuments {
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = true
			}
		}
		dbb.mutex.Unlock()
		var errors uint64
		dstTableName := dbb.DocumentsPrefix + table + dbb.DocumentsSuffix
		rows, err := dbb.dstSQLDriver.GetLimited(dstTableName, dbb.UpdateRecords)
		if err != nil {
			if err != sql.ErrNoRows {
				dbb.errlog.Println("GetLimited - Table:", dstTableName, err)
				errors++
			}
		} else {
			columns, err := rows.Columns()
			if err != nil {
				dbb.errlog.Println("Columns - Table:", dstTableName, err)
				errors++
			}
			data := make([]interface{}, len(columns))
			ptrs := make([]interface{}, len(columns))
			for index := range columns {
				ptrs[index] = &data[index]
			}
			for rows.Next() {
				err := rows.Scan(ptrs...)
				if err != nil {
					dbb.errlog.Println("Scan - Table:", dstTableName, err)
					errors++
				} else {
					var id string
					switch v := data[0].(type) {
					case []byte:
						id = string(v)
					case string:
						id = v
					case int64:
						id = strconv.FormatInt(v, 10)
					}
					row, err := dbb.srcSQLDriver.GetByID(table, data[0])
					if err != nil {
						dbb.errlog.Println("[Table:", table, "ID:"+id+"]", err)
						errors++
					}
					srcData := make([]interface{}, len(columns))
					srcPtrs := make([]interface{}, len(columns))
					for index := range columns {
						srcPtrs[index] = &srcData[index]
					}
					err = row.Scan(srcPtrs...)
					if err != nil {
						if err != sql.ErrNoRows {
							dbb.errlog.Println("[Table:", table, "ID:"+id+"]", err)
						} else {
							dbb.errlog.Println("[Table:", table, "ID:"+id+"]",
								"Original record does not exist.")
						}
						errors++
					} else {
						update := false
						for key, value := range data {
							if value == nil && srcData[key] != nil || value != nil && srcData[key] == nil {
								dbb.stdlog.Println("[Table:", table, "ID:"+id,
									"] Will be updated "+columns[key]+":", value, "->", srcData[key])
								update = true
							} else {
								if value != nil && srcData[key] != nil {
									switch vdst := value.(type) {
									case []byte:
										switch vsrc := srcData[key].(type) {
										case []byte:
											if string(vdst) != string(vsrc) {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", string(vdst), "->", string(vsrc))
												update = true
											}
										case string:
											if string(vdst) != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", string(vdst), "->", vsrc)
												update = true
											}
										case int64:
											if v, err := strconv.ParseInt(string(vdst), 10, 64); err == nil && v != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", v, "->", vsrc)
												update = true
											}
										default:
											dbb.errlog.Println("[Table:", table, "ID:"+id,
												"] Incompatible values:", value, "->", srcData[key])
											errors++
										}
									case int64:
										switch vsrc := srcData[key].(type) {
										case []byte:
											if v, err := strconv.ParseInt(string(vsrc), 10, 64); err == nil && v != vdst {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", v)
												update = true
											}
										case string:
											if v, err := strconv.ParseInt(vsrc, 10, 64); err == nil && v != vdst {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", v)
												update = true
											}
										case int64:
											if vdst != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", vsrc)
												update = true
											}
										default:
											dbb.errlog.Println("[Table:", table, "ID:"+id,
												"] Incompatible values:", value, "->", srcData[key])
											errors++
										}
									case string:
										switch vsrc := srcData[key].(type) {
										case string:
											if vdst != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", vsrc)
												update = true
											}
										case []byte:
											if vdst != string(vsrc) {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", string(vsrc))
												update = true
											}
										case int64:
											if v, err := strconv.ParseInt(vdst, 10, 64); err == nil && v != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", v, "->", vsrc)
												update = true
											}
										default:
											dbb.errlog.Println("[Table:", table, "ID:"+id,
												"] Incompatible values:", value, "->", srcData[key])
											errors++
										}
									case time.Time:
										switch vsrc := srcData[key].(type) {
										case time.Time:
											if vdst != vsrc {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", vsrc)
												update = true
											}
										case []byte:
											if t, err := time.Parse("2006-01-02 00:00:00", string(vsrc)); err == nil && t != vdst {
												dbb.stdlog.Println("[Table:", table, "ID:"+id,
													"] Will be updated "+columns[key]+":", vdst, "->", t)
												update = true
											}
										default:
											dbb.errlog.Println("[Table:", table, "ID:"+id,
												"] Incompatible values:", value, "->", srcData[key])
											errors++
										}
									default:
										dbb.errlog.Println("[Table:", table, "ID:"+id,
											"] Incompatible values:", value, "->", srcData[key])
										errors++
									}
								}
							}
						}
						if update {
							count, err := dbb.dstSQLDriver.Update(dstTableName, columns, srcData)
							if err != nil {
								dbb.errlog.Println("Update - Table:", dstTableName, err)
								errors++
							} else {
								dbb.mutex.Lock()
								for key, status := range dbb.status {
									if status.Table == table {
										dbb.status[key].Updated += count
									}
								}
								dbb.mutex.Unlock()
							}
						}
					}
				}
				select {
				case <-dbb.done:
					dbb.stdlog.Println("SQL updating gracefully done")
					return
				default:
				}
			}
		}
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Errors += errors
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = false
			}
		}
		dbb.mutex.Unlock()
	}
}

func (dbb *DBBundle) fetchSQLHandler() {
	dbb.wg.Add(1)
	defer dbb.wg.Done()
	for _, table := range dbb.InsertDocuments {
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = true
			}
		}
		dbb.mutex.Unlock()

		var errors, srcID, dstID uint64
		var err error
		dstTableName := dbb.DocumentsPrefix + table + dbb.DocumentsSuffix
		srcID, err = dbb.srcSQLDriver.LastID(table)
		if err != nil {
			dbb.errlog.Println("LastID - Table:", table, err)
			errors++
		}
		if dbb.StopBeforeID != 0 && srcID > dbb.StopBeforeID {
			srcID = dbb.StopBeforeID
		}
		switch dbb.direction {
		case sql2file:
			dstID, err = dbb.dstFileDriver.LastID(dstTableName)
		case sql2doc:
			dstID, err = dbb.dstDocumentDriver.LastID(dstTableName)
		case sql2sql:
			dstID, err = dbb.dstSQLDriver.LastID(dstTableName)
		}
		if err != nil {
			dbb.errlog.Println("LastID - Table:", dstTableName, err)
			errors++
		}
		if dstID < dbb.StartAfterID {
			dstID = dbb.StartAfterID
		}
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].LastID = dstID
			}
		}
		dbb.mutex.Unlock()
		if dstID < srcID {
			for {
				rows, err := dbb.srcSQLDriver.GetLimitedAfterID(table, dstID, dbb.InsertRecords)
				if err != nil {
					dbb.errlog.Println("GetLimitedAfterID - Table:", table, err)
					errors++
				} else {
					columns, err := rows.Columns()
					if err != nil {
						dbb.errlog.Println("Columns - Table:", table, err)
						errors++
					}
					data := make([]interface{}, len(columns))
					ptrs := make([]interface{}, len(columns))
					for index := range columns {
						ptrs[index] = &data[index]
					}
					for rows.Next() {
						err := rows.Scan(ptrs...)
						if err != nil {
							dbb.errlog.Println(err)
							errors++
						} else {
							var last uint64
							switch dbb.direction {
							case sql2file:
								last, err = dbb.dstFileDriver.AddFromSQL(dstTableName, columns, data)
							case sql2doc:
								last, err = dbb.dstDocumentDriver.AddFromSQL(dstTableName, columns, data)
							case sql2sql:
								last, err = dbb.dstSQLDriver.Insert(dstTableName, columns, data)
							}
							if err != nil {
								dbb.errlog.Println("Insert - Table:", dstTableName, err)
								errors++
							} else {
								dbb.mutex.Lock()
								for key, status := range dbb.status {
									if status.Table == table {
										dbb.status[key].Inserted++
										dbb.status[key].LastID = last
										dstID = last
									}
								}
								dbb.mutex.Unlock()
							}
						}
					}
					err = rows.Err()
					if err != nil {
						dbb.errlog.Println("Rows - Table:", table, err)
						errors++
					}
					err = rows.Close()
					if err != nil {
						dbb.errlog.Println("Close - Table:", table, err)
						errors++
					}
					if dbb.direction == sql2file {
						err = dbb.dstFileDriver.Close()
						if err != nil {
							dbb.errlog.Println("Close files:", table, err)
							errors++
						}
					}
				}
				select {
				case <-dbb.done:
					dbb.stdlog.Println("SQL fetching gracefully done")
					return
				default:
				}
				if dstID >= srcID {
					break
				}
			}
		}
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Errors += errors
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = false
			}
		}
		dbb.mutex.Unlock()
	}
}

func (dbb *DBBundle) syncFileToFileHandler() {
	dbb.wg.Add(1)
	defer dbb.wg.Done()
	// scan specified directories (topics)
	root := dbb.FileDataDir + string(os.PathSeparator) + dbb.SrcFileBucket + string(os.PathSeparator)
	for _, topic := range dbb.SrcFileTopics {
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == topic {
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = true
			}
		}
		dbb.mutex.Unlock()
		var errors uint64
		files, err := dbb.srcFileDriver.GetFiles(root+topic, dbb.DocumentsSyncCount)
		if err != nil {
			dbb.errlog.Println(err)
			if len(files) == 0 {
				dbb.stdlog.Println("Will trying to take the files the next time")
			} else {
				errors++
			}
		}
		for name, stream := range files {
			select {
			case <-dbb.done:
				err := binding.Close(files)
				if err != nil {
					dbb.errlog.Println(err)
				}
				dbb.stdlog.Println("Syncing gracefully done")
				return
			default:
			}
			dbb.stdlog.Println("Syncing of", name)
			err := dbb.dstFileDriver.PutFile(name, stream)
			if err != nil {
				dbb.errlog.Println(err)
				errors++
			} else {
				dbb.mutex.Lock()
				for key, status := range dbb.status {
					if status.Table == topic {
						dbb.status[key].Inserted++
					}
				}
				dbb.mutex.Unlock()
			}
			delete(files, name)
			if dbb.SrcFileRemove && err == nil {
				err := dbb.srcFileDriver.Remove(stream.Handle.Name())
				if err != nil {
					dbb.errlog.Println(err)
					errors++
				}
			}
		}
		dbb.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == topic {
				dbb.status[key].Errors += errors
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = false
			}
		}
		dbb.mutex.Unlock()
	}
	dbb.stdlog.Println("Syncing done")
}
