package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

// Postgres driver
type Postgres struct {
	driver *sql.DB
}

// New creates Postgres driver
func New(host string, port uint64, database, username, password string) (db *Postgres, err error) {
	db = new(Postgres)
	db.driver, err = sql.Open("postgres", "postgres://"+username+":"+password+
		"@"+host+":"+fmt.Sprintf("%d", port)+"/"+database+"?sslmode=disable")
	return
}

// LastID implements interface for getting last ID in database table
func (db *Postgres) LastID(table string) (id uint64, err error) {
	err = db.driver.QueryRow("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1").Scan(&id)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

// GetByID implements interface for getting table row by ID
func (db *Postgres) GetByID(table string, ID interface{}) (*sql.Row, error) {
	return db.driver.QueryRow("SELECT * FROM "+table+" WHERE id = $1", ID), nil
}

// GetLimited implements interface for getting last limited table rows by ID
func (db *Postgres) GetLimited(table string, limit uint64) (*sql.Rows, error) {
	return db.driver.Query("SELECT * FROM " + table + " ORDER BY id DESC LIMIT " + strconv.FormatUint(limit, 10))
}

// GetLimitedAfterID implements interface for getting limited table rows after specified ID
func (db *Postgres) GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error) {
	return db.driver.Query("SELECT * FROM " + table + " WHERE id > " +
		strconv.FormatUint(after, 10) + " ORDER BY id ASC LIMIT " + strconv.FormatUint(limit, 10))
}

// Update implements interface for updating table rows
func (db *Postgres) Update(table string, columns []string, values []interface{}) (count uint64, err error) {
	var q []string
	for index, column := range columns {
		q = append(q, column+" = $"+fmt.Sprintf("%d", index+1))
	}
	values = append(values, values[0])
	stmt, err := db.driver.Prepare("UPDATE " + table + " SET " + strings.Join(q, ",") +
		" WHERE " + columns[0] + " = $" + fmt.Sprintf("%d", len(columns)+1))
	if err != nil {
		return
	}
	res, err := stmt.Exec(values...)
	if err != nil {
		return
	}

	c, err := res.RowsAffected()
	if err != nil {
		return
	}

	err = stmt.Close()

	return uint64(c), err
}

// Insert implements interface for inserting table rows
func (db *Postgres) Insert(table string, columns []string, values []interface{}) (last uint64, err error) {
	var q []string
	for index := range columns {
		q = append(q, "$"+fmt.Sprintf("%d", index+1))
	}
	stmt, err := db.driver.Prepare("INSERT INTO " + table + "(" + strings.Join(columns, ",") +
		") VALUES (" + strings.Join(q, ",") + ") RETURNING id")
	if err != nil {
		return
	}
	err = stmt.QueryRow(values...).Scan(&last)
	if err != nil {
		return
	}

	err = stmt.Close()

	return
}
