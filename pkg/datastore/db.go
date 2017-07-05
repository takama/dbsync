package datastore

import (
	"database/sql"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/takama/dbsync/pkg/datastore/b2"
	"github.com/takama/dbsync/pkg/datastore/file"
	"github.com/takama/dbsync/pkg/datastore/mysql"
	"github.com/takama/dbsync/pkg/datastore/postgres"
	"github.com/takama/dbsync/pkg/datastore/s3"
	"github.com/takama/envconfig"
)

// DBHandler provide a simple handler interface for DB
type DBHandler interface {
	Run()
	Report() []Status
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

type replication interface {
	LastID(table string) (uint64, error)
	GetByID(table string, ID interface{}) (*sql.Row, error)
	GetLimited(table string, limit uint64) (*sql.Rows, error)
	GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error)
	Update(table string, columns []string, values []interface{}) (count uint64, err error)
	Insert(table string, columns []string, values []interface{}) (lastID uint64, err error)
}

// DBBundle contains drivers/tables information
type DBBundle struct {
	mutex     sync.RWMutex
	stdlog    *log.Logger
	errlog    *log.Logger
	status    []Status
	srcDriver replication
	dstDriver replication
	report    struct {
		mutex sync.RWMutex
	}

	// ENV vars
	SrcDbDriver   string `split_words:"true" required:"true"`
	SrcDbHost     string `split_words:"true"`
	SrcDbPort     uint64 `split_words:"true"`
	SrcDbName     string `split_words:"true"`
	SrcDbUsername string `split_words:"true"`
	SrcDbPassword string `split_words:"true"`

	DstDbDriver   string `split_words:"true" required:"true"`
	DstDbHost     string `split_words:"true"`
	DstDbPort     uint64 `split_words:"true"`
	DstDbName     string `split_words:"true"`
	DstDbUsername string `split_words:"true"`
	DstDbPassword string `split_words:"true"`

	DstAccountID   string      `split_words:"true"`
	DstAppKey      string      `split_words:"true"`
	DstFilePath    file.Fields `split_words:"true"`
	DstFileHeader  file.Fields `split_words:"true"`
	DstFileColumns file.Fields `split_words:"true"`

	UpdateTables []string `split_words:"true"`
	InsertTables []string `split_words:"true"`
	TablePrefix  string   `envconfig:"DBSYNC_DST_DB_TABLES_PREFIX"`
	TablePostfix string   `envconfig:"DBSYNC_DST_DB_TABLES_POSTFIX"`
	StartAfterID uint64   `split_words:"true"`

	UpdatePeriod uint64 `split_words:"true" required:"true"`
	InsertPeriod uint64 `split_words:"true" required:"true"`
	UpdateRows   uint64 `split_words:"true" required:"true"`
	InsertRows   uint64 `split_words:"true" required:"true"`
}

// New creates new server
func New() (*DBBundle, error) {

	bundle := &DBBundle{
		stdlog: log.New(os.Stdout, "[DBSYNC:INFO]: ", log.LstdFlags),
		errlog: log.New(os.Stderr, "[DBSYNC:ERROR]: ", log.LstdFlags),
	}
	err := envconfig.Process("dbsync", bundle)
	if err != nil {
		return nil, err
	}

	bundle.stdlog.Println(bundle)

	for _, table := range bundle.UpdateTables {
		if !bundle.exists(table) {
			bundle.status = append(bundle.status, Status{Table: table})
		}
	}
	for _, table := range bundle.InsertTables {
		if !bundle.exists(table) {
			bundle.status = append(bundle.status, Status{Table: table})
		}
	}

	switch strings.ToLower(bundle.SrcDbDriver) {
	case "b2":
		return nil, b2.ErrUnsupported
	case "s3":
		return nil, s3.ErrUnsupported
	case "pgsql":
		bundle.srcDriver, err = postgres.New(
			bundle.SrcDbHost, bundle.SrcDbPort, bundle.SrcDbName,
			bundle.SrcDbUsername, bundle.SrcDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "mysql":
		fallthrough
	default:
		bundle.srcDriver, err = mysql.New(
			bundle.SrcDbHost, bundle.SrcDbPort, bundle.SrcDbName,
			bundle.SrcDbUsername, bundle.SrcDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	}
	switch strings.ToLower(bundle.DstDbDriver) {
	case "b2":
		bundle.dstDriver, err = b2.New(
			bundle.DstAccountID, bundle.DstAppKey,
			bundle.DstFilePath, bundle.DstFileHeader, bundle.DstFileColumns...,
		)
		if err != nil {
			return bundle, err
		}
	case "s3":
		return nil, s3.ErrUnsupported
	case "pgsql":
		bundle.dstDriver, err = postgres.New(
			bundle.DstDbHost, bundle.DstDbPort, bundle.DstDbName,
			bundle.DstDbUsername, bundle.DstDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	case "mysql":
		fallthrough
	default:
		bundle.dstDriver, err = mysql.New(
			bundle.DstDbHost, bundle.DstDbPort, bundle.DstDbName,
			bundle.DstDbUsername, bundle.DstDbPassword,
		)
		if err != nil {
			return bundle, err
		}
	}

	return bundle, err
}

// Run implements interface that starts synchronization of the tables
func (dbb *DBBundle) Run() {
	go func() {
		dbb.updateHandler(dbb.UpdateRows)
		dbb.insertHandler(dbb.InsertRows)
		// setup handlers
		updateTicker := time.NewTicker(time.Duration(dbb.UpdatePeriod) * time.Second)
		go func() {
			for range updateTicker.C {
				dbb.updateHandler(dbb.UpdateRows)
			}
		}()
		insertTicker := time.NewTicker(time.Duration(dbb.InsertPeriod) * time.Second)
		go func() {
			for range insertTicker.C {
				dbb.insertHandler(dbb.InsertRows)
			}
		}()
	}()
}

// Report implements interface that shows status detailed information
func (dbb *DBBundle) Report() []Status {
	dbb.report.mutex.RLock()
	var status []Status
	status = append(status, dbb.status...)
	dbb.report.mutex.RUnlock()
	for key, value := range status {
		status[key].Duration = time.Now().Sub(value.Changed).String()
	}
	return status
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

func (dbb *DBBundle) updateHandler(updateRows uint64) {
	dbb.mutex.Lock()
	defer dbb.mutex.Unlock()
	for _, table := range dbb.UpdateTables {
		dbb.report.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = true
			}
		}
		dbb.report.mutex.Unlock()
		var errors uint64
		dstTableName := dbb.TablePrefix + table + dbb.TablePostfix
		rows, err := dbb.dstDriver.GetLimited(dstTableName, updateRows)
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
					row, err := dbb.srcDriver.GetByID(table, data[0])
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
							count, err := dbb.dstDriver.Update(dstTableName, columns, srcData)
							if err != nil {
								dbb.errlog.Println("Update - Table:", dstTableName, err)
								errors++
							} else {
								dbb.report.mutex.Lock()
								for key, status := range dbb.status {
									if status.Table == table {
										dbb.status[key].Updated += count
									}
								}
								dbb.report.mutex.Unlock()
							}
						}
					}
				}
			}
		}
		dbb.report.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Errors += errors
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = false
			}
		}
		dbb.report.mutex.Unlock()
	}
}

func (dbb *DBBundle) insertHandler(insertRows uint64) {
	dbb.mutex.Lock()
	defer dbb.mutex.Unlock()
	for _, table := range dbb.InsertTables {
		dbb.report.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = true
			}
		}
		dbb.report.mutex.Unlock()

		var errors uint64
		dstTableName := dbb.TablePrefix + table + dbb.TablePostfix
		srcID, err := dbb.srcDriver.LastID(table)
		if err != nil {
			dbb.errlog.Println("LastID - Table:", table, err)
			errors++
		}
		dstID, err := dbb.dstDriver.LastID(dstTableName)
		if err != nil {
			dbb.errlog.Println("LastID - Table:", dstTableName, err)
			errors++
		}
		if dstID < dbb.StartAfterID {
			dstID = dbb.StartAfterID
		}
		dbb.report.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].LastID = dstID
			}
		}
		dbb.report.mutex.Unlock()
		if dstID < srcID {
			for {
				rows, err := dbb.srcDriver.GetLimitedAfterID(table, dstID, insertRows)
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
							last, err := dbb.dstDriver.Insert(dstTableName, columns, data)
							if err != nil {
								dbb.errlog.Println("Insert - Table:", dstTableName, err)
								errors++
							} else {
								dbb.report.mutex.Lock()
								for key, status := range dbb.status {
									if status.Table == table {
										dbb.status[key].Inserted++
										dbb.status[key].LastID = last
										dstID = last
									}
								}
								dbb.report.mutex.Unlock()
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
				}
				if dstID >= srcID {
					break
				}
			}
		}
		dbb.report.mutex.Lock()
		for key, status := range dbb.status {
			if status.Table == table {
				dbb.status[key].Errors += errors
				dbb.status[key].Changed = time.Now()
				dbb.status[key].Running = false
			}
		}
		dbb.report.mutex.Unlock()
	}
}
