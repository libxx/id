package generator

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"time"
)

const (
	selectSQL      = "SELECT `value` FROM %s WHERE `key` = ? FOR UPDATE"
	insertSQL      = "INSERT INTO %s (`key`, `value`, `last_mod_at`) values (?, ?, ?)"
	updateSQL      = "UPDATE %s SET `value` = ?, `last_mod_at` = ? WHERE `key` = ?"
	createTableSQL = "CREATE TABLE %s (\n" +
		"	`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
		"	`key` VARCHAR(32) NOT NULL,\n" +
		"	`value` INT UNSIGNED NOT NULL,\n" +
		"	`last_mod_at` INT UNSIGNED NOT NULL,\n" +
		"	PRIMARY KEY (`id`),\n" +
		"	UNIQUE KEY `key` (`key`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
)

type MysqlConfig struct {
	Dsn       string
	TableName string
}

func InitMysqlEngine(config MysqlConfig) (err error) {
	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(createTableSQL, config.TableName))
	return
}

func NewMysqlEngine(config MysqlConfig, key string, skip int64) (engine Engine, err error) {
	if skip <= 0 {
		err = fmt.Errorf("invalid skip: %d", skip)
		return
	}

	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}

	mysqlEngine := new(mysqlEngine)
	mysqlEngine.db = db
	mysqlEngine.config = config
	mysqlEngine.selectSQL = fmt.Sprintf(selectSQL, config.TableName)
	mysqlEngine.insertSQL = fmt.Sprintf(insertSQL, config.TableName)
	mysqlEngine.updateSQL = fmt.Sprintf(updateSQL, config.TableName)
	mysqlEngine.skip = skip
	mysqlEngine.key = key
	mysqlEngine.cur, mysqlEngine.max, err = mysqlEngine.increment(skip)
	if err != nil {
		return
	}

	return mysqlEngine, err
}

type mysqlEngine struct {
	db        *sql.DB
	config    MysqlConfig
	selectSQL string
	updateSQL string
	insertSQL string
	key       string
	skip      int64
	max       int64
	cur       int64
	mutex     sync.Mutex
}

func (m *mysqlEngine) Next() (id int64, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.cur == m.max {
		m.cur, m.max, err = m.increment(m.skip)
		if err != nil {
			return
		}
	}

	m.cur++
	return m.cur, nil
}

func (m *mysqlEngine) Current() (int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.cur, nil
}

func (m *mysqlEngine) Close() error {
	return m.db.Close()
}

func (m *mysqlEngine) increment(delta int64) (cur, max int64, err error) {
	tx, err := m.db.Begin()
	defer func() {
		if err != nil {
			newErr := tx.Rollback()
			if newErr != nil {
				err = newErr
			}
		} else {
			err = tx.Commit()
		}
	}()
	if err != nil {
		return
	}
	err = m.db.QueryRow(m.selectSQL, m.key).Scan(&cur)
	if err != nil {
		if err == sql.ErrNoRows {
			var res sql.Result
			max += m.skip
			res, err = m.db.Exec(m.insertSQL, m.key, max, time.Now().Unix())
			var cnt int64
			cnt, err = res.RowsAffected()
			if err != nil {
				return
			}

			if cnt != 1 {
				err = fmt.Errorf("invalid effected row count: %d", cnt)
			}
		}
		return
	}

	max = cur + delta
	res, err := m.db.Exec(m.updateSQL, max, time.Now().Unix(), m.key)
	if err != nil {
		return
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		return
	}

	if cnt != 1 {
		err = fmt.Errorf("invalid effected row count: %d", cnt)
	}

	return
}
