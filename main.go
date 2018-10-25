package main

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"gopkg.in/yaml.v2"
)

type bigqueryRow struct {
	row      map[string]bigquery.Value
	insertID string
}

func (r bigqueryRow) Save() (map[string]bigquery.Value, string, error) {
	return r.row, r.insertID, nil
}

type handler struct {
	client  *bigquery.Client
	dataset string
}

func (h *handler) OnRotate(*replication.RotateEvent) error          { return nil }
func (h *handler) OnTableChanged(schema string, table string) error { return nil }
func (h *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}
func (h *handler) OnRow(e *canal.RowsEvent) error {
	switch e.Action {
	case canal.InsertAction:
		h.insert(e)
	case canal.DeleteAction:
		//h.delete(e)
	case canal.UpdateAction:
		h.update(e)
	default:
		panic("unknown action")
	}

	return nil
}
func (h *handler) OnXID(mysql.Position) error             { return nil }
func (h *handler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *handler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *handler) String() string                         { return "handler" }

func (h *handler) insert(e *canal.RowsEvent) {
	rows := make([]interface{}, 0, len(e.Rows))

	for i := 0; i < len(e.Rows); i++ {
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

	ctx := context.Background()

	u := h.client.Dataset(h.dataset).Table(e.Table.Name).Uploader()
	if err := u.Put(ctx, rows); err != nil {
		panic(err)
	} else {
		print(",")
	}
}

func (h *handler) update(e *canal.RowsEvent) {
	rows := make([]interface{}, 0, len(e.Rows))

	// e.Rows[0] is the old value, e.Rows[1] is the new value.
	for i := 0; i < len(e.Rows); i += 2 {
		row := bigqueryRow{
			row:      make(map[string]bigquery.Value, len(e.Rows[i+1])),
			insertID: strconv.FormatUint(uint64(e.Header.LogPos), 10),
		}

		for j := 0; j < len(e.Rows[i+1]); j++ {
			column := e.Table.Columns[j]
			row.row[column.Name] = bigqueryValue(column, e.Rows[i+1][j])
		}

		rows = append(rows, row)
	}

	ctx := context.Background()

	u := h.client.Dataset(h.dataset).Table(e.Table.Name).Uploader()
	if err := u.Put(ctx, rows); err != nil {
		if pme, ok := err.(bigquery.PutMultiError); ok {
			panic(pme[0].Error())
		} else {
			panic(err)
		}
	} else {
		print(".")
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

type Config struct {
	Addr     string   `yaml:"addr"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	ServerID uint32   `yaml:"serverid"`
	Tables   []string `yaml:"tables"`
	Project  string   `yaml:"project"`
	Dataset  string   `yaml:"dataset"`
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

	cfg.IncludeTableRegex = config.Tables

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}

	bc, err := bigquery.NewClient(context.Background(), config.Project)
	if err != nil {
		panic(err)
	}

	canal.SetEventHandler(&handler{
		client:  bc,
		dataset: config.Dataset,
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
