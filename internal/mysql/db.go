package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/util"
)

type dbWorker struct {
	db      *sqlx.DB
	logger  *log.Logger
	host    string
	config  *config.Config
	version *Version
}

func newDbWoker(config *config.Config, logger *log.Logger, host string) (*dbWorker, error) {
	addr := util.JoinHostPort(host, config.MySQL.Port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/mysql", config.MySQL.User, config.MySQL.Password, addr)
	if config.MySQL.SslCA != "" {
		dsn += "?tls=custom"
	}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// Unsafe option allow us to use queries containing fields missing in structs
	// eg. when we running "SHOW SLAVE STATUS", but need only few columns
	db = db.Unsafe()
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(3)
	db.SetConnMaxLifetime(3 * config.TickInterval)
	return &dbWorker{
		db:     db,
		logger: logger,
		config: config,
		host:   host,
	}, nil
}

// nolint: unparam
func (n *dbWorker) execWithTimeout(queryName string, arg map[string]interface{}, timeout time.Duration) error {
	if arg == nil {
		arg = map[string]interface{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// avoid connection leak on long lock timeouts
	lockTimeout := int64(math.Floor(0.8 * float64(timeout/time.Second)))
	if _, err := n.db.ExecContext(ctx, n.getQuery(querySetLockTimeout), lockTimeout); err != nil {
		n.traceQuery(query, arg, nil, err)
		return err
	}

	_, err := n.db.NamedExecContext(ctx, query, arg)
	n.traceQuery(query, arg, nil, err)
	return err
}

// nolint: unparam
func (n *dbWorker) exec(queryName string, arg map[string]interface{}) error {
	return n.execWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *dbWorker) getQuery(name string) string {
	query, ok := n.config.Queries[name]
	if !ok {
		query, ok = DefaultQueries[name]
	}
	if !ok {
		panic(fmt.Sprintf("Failed to find query with name '%s'", name))
	}
	return query
}

func (n *dbWorker) traceQuery(query string, arg interface{}, result interface{}, err error) {
	query = queryOnliner.ReplaceAllString(query, " ")
	msg := fmt.Sprintf("node %s running query '%s' with args %#v, result: %#v, error: %v", n.host, query, arg, result, err)
	msg = strings.Replace(msg, n.config.MySQL.Password, "********", -1)
	msg = strings.Replace(msg, n.config.MySQL.ReplicationPassword, "********", -1)
	n.logger.Debug(msg)
}

//nolint:unparam
func (n *dbWorker) queryRow(queryName string, arg interface{}, result interface{}) error {
	return n.queryRowWithTimeout(queryName, arg, result, n.config.DBTimeout)
}

func (n *dbWorker) queryRowWithTimeout(queryName string, arg interface{}, result interface{}, timeout time.Duration) error {
	if arg == nil {
		arg = struct{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	if err == nil {
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			err = rows.StructScan(result)
		} else {
			err = rows.Err()
			if err == nil {
				err = sql.ErrNoRows
			}
		}
	}
	n.traceQuery(query, arg, result, err)
	return err
}

// nolint: unparam
func (n *dbWorker) queryRows(queryName string, arg interface{}, scanner func(*sqlx.Rows) error) error {
	if arg == nil {
		arg = struct{}{}
	}
	query := n.getQuery(queryName)
	ctx, cancel := context.WithTimeout(context.Background(), n.config.DBTimeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	n.traceQuery(query, arg, rows, err)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = scanner(rows)
		if err != nil {
			break
		}
	}
	return err
}

func (n *dbWorker) getRunningQueryIds(excludeUsers []string, timeout time.Duration) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := DefaultQueries[queryGetProcessIds]

	bquery, args, err := sqlx.In(query, excludeUsers)
	if err != nil {
		n.traceQuery(bquery, args, nil, err)
		return nil, err
	}
	rows, err := n.db.QueryxContext(ctx, bquery, args...)
	if err != nil {
		n.traceQuery(bquery, args, nil, err)
		return nil, err
	}
	defer rows.Close()
	var ret []int

	for rows.Next() {
		var currid int
		err := rows.Scan(&currid)
		if err != nil {
			n.traceQuery(bquery, nil, ret, err)
			return nil, err
		}
		ret = append(ret, currid)
	}

	n.traceQuery(bquery, args, ret, nil)

	return ret, nil
}

// Poorman's sql templating with value quotation
// Because go built-in placeholders don't work for queries like CHANGE MASTER
func Mogrify(query string, arg map[string]interface{}) string {
	return mogrifyRegex.ReplaceAllStringFunc(query, func(n string) string {
		n = n[1:]
		if v, ok := arg[n]; ok {
			switch vt := v.(type) {
			case schemaname:
				return "`" + string(vt) + "`"
			case string:
				return "'" + escape(vt) + "'"
			case int:
				return strconv.Itoa(vt)
			default:
				return "'" + escape(fmt.Sprintf("%s", vt)) + "'"
			}
		} else {
			return n
		}
	})
}

// not all queries may be parametrized with placeholders
func (n *dbWorker) execMogrifyWithTimeout(queryName string, arg map[string]interface{}, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := n.db.ExecContext(ctx, query)
	n.traceQuery(query, nil, nil, err)
	return err
}

func (n *dbWorker) execMogrify(queryName string, arg map[string]interface{}) error {
	return n.execMogrifyWithTimeout(queryName, arg, n.config.DBTimeout)
}

func (n *dbWorker) queryRowMogrifyWithTimeout(queryName string, arg map[string]interface{}, result interface{}, timeout time.Duration) error {
	query := n.getQuery(queryName)
	query = Mogrify(query, arg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := n.db.NamedQueryContext(ctx, query, arg)
	if err == nil {
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			err = rows.StructScan(result)
		} else {
			err = rows.Err()
			if err == nil {
				err = sql.ErrNoRows
			}
		}
	}
	n.traceQuery(query, arg, result, err)
	return err
}

func (n *dbWorker) GetVersion() (*Version, error) {
	if n.version != nil {
		return n.version, nil
	}
	v := new(Version)
	err := n.queryRow(queryGetVersion, nil, v)
	if err != nil {
		return nil, err
	}
	n.version = v
	return n.version, nil
}
