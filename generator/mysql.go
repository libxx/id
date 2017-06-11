package generator

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/libxx/id/logging"
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

func InitMysqlGenerator(config MysqlConfig) (err error) {
	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(createTableSQL, config.TableName))
	return
}

func NewMysqlGenerator(config MysqlConfig, skip int64, logFunc logging.LogFunc) (generator Generator, err error) {
	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}

	g := new(MysqlGenerator)
	g.sourceMap = make(map[string]*mysqlRowBasedEngine)
	g.db = db
	g.config = config
	g.skip = skip
	g.logFunc = logging.NewWrapperLogFunc(logFunc)
	generator = g
	return
}

type MysqlGenerator struct {
	sync.RWMutex
	sourceMap map[string]*mysqlRowBasedEngine
	db        *sql.DB
	config    MysqlConfig
	skip      int64
	logFunc   logging.LogFunc
}

func (m *MysqlGenerator) EnableKeys(keys []string) (err error) {
	data := make(map[string]*mysqlRowBasedEngine, len(keys))
	for _, key := range keys {
		data[key], err = newMysqlRowBasedEngine(m, key, m.skip, m.logFunc)
		if err != nil {
			return
		}
	}
	m.Lock()
	defer m.Unlock()

	m.sourceMap = data
	return
}

func (m *MysqlGenerator) Next(key string) (id int64, err error) {
	engine, err := m.rowBasedEngine(key)
	if err != nil {
		return
	}
	return engine.next()
}

func (m *MysqlGenerator) Current(key string) (id int64, err error) {
	engine, err := m.rowBasedEngine(key)
	if err != nil {
		return
	}
	return engine.current()
}

func (m *MysqlGenerator) Close() error {
	return m.db.Close()
}

func (m *MysqlGenerator) rowBasedEngine(key string) (engine *mysqlRowBasedEngine, err error) {
	m.RLock()
	defer m.RUnlock()
	engine, exist := m.sourceMap[key]
	if !exist {
		err = ErrKeyDoesNotExist
	}
	return
}

func newMysqlRowBasedEngine(generator *MysqlGenerator, key string, skip int64, logFunc logging.LogFunc) (engine *mysqlRowBasedEngine, err error) {
	if skip <= 0 {
		err = fmt.Errorf("invalid skip: %d", skip)
		return
	}

	mysqlEngine := new(mysqlRowBasedEngine)
	mysqlEngine.generator = generator
	mysqlEngine.selectSQL = fmt.Sprintf(selectSQL, generator.config.TableName)
	mysqlEngine.insertSQL = fmt.Sprintf(insertSQL, generator.config.TableName)
	mysqlEngine.updateSQL = fmt.Sprintf(updateSQL, generator.config.TableName)
	mysqlEngine.skip = skip
	mysqlEngine.key = key
	mysqlEngine.logFunc = logFunc
	logFunc(fmt.Sprintf("initialize counter for key: \"%s\"", key))
	mysqlEngine.cur, mysqlEngine.max, err = mysqlEngine.increase(skip)
	if err != nil {
		return
	}

	return mysqlEngine, err
}

type mysqlRowBasedEngine struct {
	generator *MysqlGenerator
	selectSQL string
	updateSQL string
	insertSQL string
	key       string
	skip      int64
	max       int64
	cur       int64
	mutex     sync.Mutex
	logFunc   logging.LogFunc
}

func (m *mysqlRowBasedEngine) next() (id int64, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.cur == m.max {
		m.logFunc(fmt.Sprintf("increase counter for key: \"%s\"", m.key))
		m.cur, m.max, err = m.increase(m.skip)
		if err != nil {
			return
		}
	}
	m.cur++
	return m.cur, nil
}

func (m *mysqlRowBasedEngine) current() (int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.cur, nil
}

func (m *mysqlRowBasedEngine) increase(delta int64) (cur, max int64, err error) {
	m.logFunc(fmt.Sprintf("before increasing counter for key: \"%s\", current: %d.", m.key, m.cur))
	defer func() {
		if err == nil {
			m.logFunc(fmt.Sprintf("after increasing counter for key: \"%s\", current: %d.", m.key, cur))
		}
	}()
	tx, err := m.generator.db.Begin()
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
	err = m.generator.db.QueryRow(m.selectSQL, m.key).Scan(&cur)
	if err != nil {
		if err == sql.ErrNoRows {
			var res sql.Result
			max += m.skip
			res, err = m.generator.db.Exec(m.insertSQL, m.key, max, time.Now().Unix())
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
	res, err := m.generator.db.Exec(m.updateSQL, max, time.Now().Unix(), m.key)
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
