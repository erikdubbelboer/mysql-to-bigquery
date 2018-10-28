package main

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
)

type bigqueryRow struct {
	row      map[string]bigquery.Value
	insertID string
}

func (r bigqueryRow) Save() (map[string]bigquery.Value, string, error) {
	return r.row, r.insertID, nil
}

type valueScanner struct {
	i interface{}
}

func (v *valueScanner) Scan(src interface{}) error {
	switch tv := src.(type) {
	case []byte:
		c := make([]byte, len(tv))
		copy(c, tv)
		v.i = c
	default:
		v.i = src
	}
	return nil
}

type handler struct {
	client *bigquery.Client
	config Config
	db     *sql.DB
}

func (h *handler) OnRotate(*replication.RotateEvent) error          { return nil }
func (h *handler) OnTableChanged(schema string, table string) error { return nil }
func (h *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}
func (h *handler) OnRow(e *canal.RowsEvent) error {
	var r Rule
	found := false
	for _, rule := range h.config.Rules {
		if rule.tableRegexp.MatchString(e.Table.Name) {
			r = rule
			found = true
			break
		}
	}

	if !found {
		panic("OnRow not maching any rule")
	}

	switch e.Action {
	case canal.InsertAction:
		if r.Update.Action == "delete" {
			h.delete(r.Update, e, 0, 1)
		} else {
			h.update(r.Update, e, 0, 1)
		}
	case canal.DeleteAction:
		if r.Update.Action == "update" {
			h.update(r.Update, e, 0, 1)
		} else {
			h.delete(r.Update, e, 0, 1)
		}
	case canal.UpdateAction:
		if r.Update.Action == "delete" {
			h.delete(r.Update, e, 1, 2)
		} else {
			h.update(r.Update, e, 1, 2)
		}
	default:
		panic("unknown action")
	}

	return nil
}
func (h *handler) OnXID(mysql.Position) error             { return nil }
func (h *handler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *handler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *handler) String() string                         { return "handler" }

func (h *handler) update(a Action, e *canal.RowsEvent, offset, increment int) {
	if a.Action == "none" {
		return
	}

	rows := make([]interface{}, 0, len(e.Rows))

	if a.Query != "" {
		for i := offset; i < len(e.Rows); i += increment {
			row := bigqueryRow{
				row:      make(map[string]bigquery.Value),
				insertID: strconv.FormatUint(uint64(e.Header.LogPos), 10),
			}

			args := make([]interface{}, 0, len(e.Rows[i]))

			for j := 0; j < len(e.Rows[i]); j++ {
				column := e.Table.Columns[j]
				args = append(args, sql.Named(column.Name, bigqueryValue(column, e.Rows[i][j])))
			}

			qr, err := h.db.Query(a.Query, args...)
			if err != nil {
				panic(err)
			}

			columns, err := qr.Columns()
			if err != nil {
				panic(err)
			}

			for qr.Next() {
				values := make([]valueScanner, len(columns))
				ifs := make([]interface{}, len(columns))

				for k, v := range values {
					ifs[k] = &v
				}

				if err := qr.Scan(ifs...); err != nil {
					panic(err)
				}

				for j := 0; j < len(columns); j++ {
					row.row[columns[j]] = values[j].i
				}

				rows = append(rows, row)
			}
		}
	} else {
		for i := offset; i < len(e.Rows); i += increment {
			row := bigqueryRow{
				row:      make(map[string]bigquery.Value, len(e.Rows[i])),
				insertID: strconv.FormatUint(uint64(e.Header.LogPos), 10),
			}

			for j := 0; j < len(e.Rows[i]); j++ {
				column := e.Table.Columns[j]
				row.row[column.Name] = bigqueryValue(column, e.Rows[i][j])
			}

			rows = append(rows, row)
		}
	}

	ctx := context.Background()

	table := a.Table
	if table == "" {
		table = e.Table.Name
	}

	u := h.client.Dataset(h.config.Dataset).Table(table).Uploader()
	if err := u.Put(ctx, rows); err != nil {
		panic(err)
	} else {
		print("-")
	}
}

func (h *handler) delete(a Action, e *canal.RowsEvent, offset, increment int) {
	if a.Action == "none" {
		return
	}

	for i := offset; i < len(e.Rows); i += increment {
		where := make([]string, 0, len(e.Table.PKColumns))
		parameters := make([]bigquery.QueryParameter, 0, len(e.Table.PKColumns))

		for _, pkc := range e.Table.PKColumns {
			column := e.Table.Columns[pkc]

			where = append(where, column.Name+" = @"+column.Name)
			parameters = append(parameters, bigquery.QueryParameter{
				Name:  column.Name,
				Value: bigqueryValue(column, e.Rows[i][pkc]),
			})
		}

		q := h.client.Query("DELETE FROM " + e.Table.Name + " WHERE " + strings.Join(where, " AND "))
		q.DefaultProjectID = h.config.Project
		q.DefaultDatasetID = h.config.Dataset
		q.Parameters = parameters

		if j, err := q.Run(context.Background()); err != nil {
			panic(err)
		} else if status, err := j.Wait(context.Background()); err != nil {
			panic(err)
		} else if err := status.Err(); err != nil {
			panic(err)
		} else {
			print("|")
		}
	}
}

func bigqueryValue(col schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				panic("invalid binlog enum index")
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		panic("json not supported")
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, _ := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			return vt.Format(time.RFC3339)
		}
	}

	return value
}

type Action struct {
	Action string `yaml:"action"`
	Query  string `yaml:"query"`
	Table  string `yaml:"table"`
}

type Rule struct {
	Table  string `yaml:"table"`
	Update Action `yaml:"update"`
	Delete Action `yaml:"delete"`

	tableRegexp *regexp.Regexp
}

type Config struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	ServerID uint32 `yaml:"serverid"`
	Project  string `yaml:"project"`
	Dataset  string `yaml:"dataset"`
	Rules    []Rule `yaml:"rules"`
}

func main() {
	if len(os.Args) < 2 {
		panic("config file not specified")
	}

	data, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	data = []byte(os.ExpandEnv(string(data)))

	var config Config

	if err := yaml.Unmarshal(data, &config); err != nil {
		panic(err)
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Password
	cfg.Charset = "utf8"
	cfg.Flavor = "mysql"
	cfg.ServerID = config.ServerID
	cfg.Dump.DiscardErr = false

	tables := make([]string, 0, len(config.Rules))
	for _, rule := range config.Rules {
		rule.tableRegexp, err = regexp.Compile(rule.Table)
		if err != nil {
			panic(err)
		}

		tables = append(tables, rule.Table)
	}

	cfg.IncludeTableRegex = tables

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}

	bc, err := bigquery.NewClient(context.Background(), config.Project)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", config.User+":"+config.Password+"@tcp("+config.Addr+":3306)/")
	if err != nil {
		panic(err)
	}

	canal.SetEventHandler(&handler{
		client: bc,
		config: config,
		db:     db,
	})

	if err := canal.CheckBinlogRowImage("FULL"); err != nil {
		panic(err)
	}

	pos, err := canal.GetMasterPos()
	if err != nil {
		panic(err)
	}

	if err := canal.RunFrom(pos); err != nil {
		panic(err)
	}
}
