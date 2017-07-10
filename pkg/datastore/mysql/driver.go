package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// MySQL driver
type MySQL struct {
	driver *sql.DB
}

// New creates MySQL driver
func New(host string, port uint64, database, username, password string) (db *MySQL, err error) {
	db = new(MySQL)
	db.driver, err = sql.Open("mysql", username+":"+password+
		"@tcp("+host+":"+fmt.Sprintf("%d", port)+")/"+database)
	return
}

// LastID implements interface for getting last ID in database table
func (db *MySQL) LastID(table string) (id uint64, err error) {
	err = db.driver.QueryRow("SELECT ID FROM " + table + " ORDER BY ID DESC LIMIT 1").Scan(&id)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

// GetByID implements interface for getting table row by ID
func (db *MySQL) GetByID(table string, ID interface{}) (*sql.Row, error) {
	return db.driver.QueryRow("SELECT * FROM "+table+" WHERE ID = ?", ID), nil
}

// GetLimited implements interface for getting last limited table rows by ID
func (db *MySQL) GetLimited(table string, limit uint64) (*sql.Rows, error) {
	return db.driver.Query("SELECT * FROM " + table + " ORDER BY ID DESC LIMIT " + strconv.FormatUint(limit, 10))
}

// GetLimitedAfterID implements interface for getting limited table rows after specified ID
func (db *MySQL) GetLimitedAfterID(table string, after, limit uint64) (*sql.Rows, error) {
	return db.driver.Query("SELECT * FROM " + table + " WHERE ID > " +
		strconv.FormatUint(after, 10) + " ORDER BY ID ASC LIMIT " + strconv.FormatUint(limit, 10))
}

// Update implements interface for updating table rows
func (db *MySQL) Update(table string, columns []string, values []interface{}) (count uint64, err error) {
	var q []string
	for _, column := range columns {
		q = append(q, column+" = ?")
	}
	values = append(values, values[0])
	stmt, err := db.driver.Prepare("UPDATE " + table + " SET " + strings.Join(q, ",") +
		" WHERE " + columns[0] + " = ?")
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
func (db *MySQL) Insert(table string, columns []string, values []interface{}) (last uint64, err error) {
	var q []string
	for range columns {
		q = append(q, "?")
	}
	stmt, err := db.driver.Prepare("INSERT INTO " + table + "(" + strings.Join(columns, ",") +
		") VALUES (" + strings.Join(q, ",") + ")")
	if err != nil {
		return
	}
	res, err := stmt.Exec(values...)
	if err != nil {
		return
	}
	l, err := res.LastInsertId()
	if err != nil {
		return
	}

	err = stmt.Close()

	return uint64(l), err
}

// Close flushes data and closes files
func (db *MySQL) Close() (err error) {
	return
}
