package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func NewBinLog(conf *RootFlag) (*BinLog, error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%d", conf.Host, conf.Port), conf.User, conf.Password, "")
	if err != nil {
		return nil, err
	}
	result, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, err
	}
	binLog := &BinLog{}
	binLog.eofFile = string(result.Values[0][0].AsString())
	binLog.eofPos = int(result.Values[0][1].AsInt64())
	result, err = conn.Execute("SHOW MASTER LOGS")
	if err != nil {
		return nil, err
	}
	binlogNameSizeM := make(map[string]int)
	for _, v := range result.Values {
		name := string(v[0].AsString())
		size := int(v[1].AsInt64())
		binlogNameSizeM[name] = size
	}
	if _, ok := binlogNameSizeM[conf.StartFile]; !ok && conf.StartFile != "" {
		return nil, fmt.Errorf("parameter error: start_file %s not in mysql server", conf.StartFile)
	}
	result, err = conn.Execute("SELECT @@server_id")
	if err != nil {
		return nil, err
	}
	binLog.serverId = uint32(result.Values[0][0].AsInt64())
	if binLog.serverId == 0 {
		return nil, fmt.Errorf("missing server_id in %s:%d", conf.Host, conf.Port)
	}
	binLog.conf = conf
	binLog.conn = conn
	return binLog, nil
}

type BinLog struct {
	conn     *client.Conn
	conf     *RootFlag
	eofFile  string
	eofPos   int
	serverId uint32

	column   []string
	columnPk map[string]struct{}
}

func (b *BinLog) Run() error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:       b.serverId,
		Flavor:         "mysql",
		Host:           b.conf.Host,
		Port:           b.conf.Port,
		User:           b.conf.User,
		Password:       b.conf.Password,
		RawModeEnabled: false,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	var pos = mysql.Position{Name: b.eofFile, Pos: uint32(b.eofPos)}
	if b.conf.StartFile != "" {
		pos.Name, pos.Pos = b.conf.StartFile, b.conf.StartPosition
	}
	streamer, _ := syncer.StartSync(pos)

	var (
		ev  *replication.BinlogEvent
		err error
	)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if !b.conf.StartDateTime.IsZero() {
			ev, err = streamer.GetEventWithStartTime(ctx, b.conf.StartDateTime)
		} else {
			ev, err = streamer.GetEvent(ctx)
		}
		cancel()
		if ev == nil && err == nil {
			continue
		}
		if nextPos := syncer.GetNextPosition(); b.conf.EndFile != "" && ((nextPos.Name > b.conf.EndFile) || nextPos.Name == b.conf.EndFile && nextPos.Pos > uint32(b.conf.EndPosition)) {
			return nil
		}
		if err == context.DeadlineExceeded {
			if !b.conf.StopNever && b.conf.StopDateTime.IsZero() {
				return nil
			}
			if !b.conf.StopDateTime.IsZero() && time.Now().Unix() > b.conf.StopDateTime.Unix() { //当前时间大于过滤的截止时间时,return
				return nil
			}
			continue

		} else if err != nil {
			return err
		}
		if event, ok := ev.Event.(*replication.TableMapEvent); ok {
			err = b.getClumns(event)
			if err != nil {
				return err
			}
		} else if event, ok := ev.Event.(*replication.RowsEvent); ok {
			if b.conf.DbName != "" && b.conf.DbName != fmt.Sprintf("%s", event.Table.Schema) {
				continue
			} else if len(b.conf.OnlyTables) != 0 {
				var flag bool
				for _, tableName := range b.conf.OnlyTables {
					if fmt.Sprintf("%s", event.Table.Table) == tableName {
						flag = true
					}
				}
				if !flag {
					continue
				}
			}
			sql := b.generate_sql_pattern(ev.Header.EventType, event, false)
			b.FPrintSql(ev.Header.Timestamp, sql)
		}
	}
}

func (b *BinLog) getClumns(e *replication.TableMapEvent) error {
	result, err := b.conn.Execute(fmt.Sprintf(`
	SELECT
		COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
		COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
	FROM
		information_schema.columns
	WHERE
		table_schema = '%s' AND table_name = '%s'
	`, string(e.Schema), string(e.Table)))
	if err != nil {
		return err
	}
	b.column = make([]string, 0)
	b.columnPk = make(map[string]struct{})
	for _, v := range result.Values {
		fieldName := fmt.Sprintf("%s", v[0].AsString())
		b.column = append(b.column, fieldName)
		if fmt.Sprintf("%s", v[5].AsString()) == "PRI" {
			b.columnPk[fieldName] = struct{}{}
		}
	}
	return nil
}

func (b *BinLog) generate_sql_pattern(eventType replication.EventType, e *replication.RowsEvent, noPk bool) (sql string) {
	rows := make([][]string, 0)
	for _, row := range e.Rows {
		tmp := make([]string, 0)
		for _, v := range row {
			t := fmt.Sprintf("%v", v)
			if _, err := strconv.Atoi(t); err != nil {
				tmp = append(tmp, "'"+t+"'")
			} else {
				tmp = append(tmp, t)
			}
		}
		rows = append(rows, tmp)
	}
	column := b.column[:len(rows[0])]
	columnSql := "`" + strings.Join(column, "`,`") + "`"
	if b.conf.Flashback {
		switch eventType {
		case replication.WRITE_ROWS_EVENTv2:
			sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1", e.Table.Schema, e.Table.Table, b.mogrify(column, rows[0], true))
		case replication.DELETE_ROWS_EVENTv2:
			sql = fmt.Sprintf("INSERT INFO `%s`.`%s`(%s) VALUES (%s)", e.Table.Schema, e.Table.Table, columnSql, strings.Join(rows[0], ","))
		case replication.UPDATE_ROWS_EVENTv2:
			sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1", e.Table.Schema, e.Table.Table, b.mogrify(column, rows[0], false), b.mogrify(column, rows[1], true))
		}
	} else {
		switch eventType {
		case replication.WRITE_ROWS_EVENTv2:
			if noPk {

			}
			sql = fmt.Sprintf("INSERT INFO `%s`.`%s`(%s) VALUES (%s)", e.Table.Schema, e.Table.Table, columnSql, strings.Join(rows[0], ","))
		case replication.DELETE_ROWS_EVENTv2:
			sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1", e.Table.Schema, e.Table.Table, b.mogrify(column, rows[0], true))
		case replication.UPDATE_ROWS_EVENTv2:
			sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1", e.Table.Schema, e.Table.Table, b.mogrify(column, rows[1], false), b.mogrify(column, rows[0], true))
		}
	}

	return sql
}

func (b *BinLog) mogrify(column, row []string, isWhere bool) string {
	s := ""

	for index, v := range row {
		if isWhere && len(b.columnPk) != 0 {
			if _, ok := b.columnPk[column[index]]; !ok {
				continue
			}
		}
		s += fmt.Sprintf("`%s`=%s  AND ", column[index], v)
	}
	s = strings.TrimRight(s, "  AND ")
	return s
}

func (b *BinLog) FPrintSql(timestamp uint32, sql string) {
	t := time.Unix(int64(timestamp), 0)
	tStr := t.Local().Format(time.RFC3339)
	pre := color.CyanString("事件时间: "+tStr) + " | "
	if strings.HasPrefix(sql, "INSERT") {
		fmt.Fprintln(color.Output, pre, color.GreenString(sql))
	} else if strings.HasPrefix(sql, "UPDATE") {
		fmt.Fprintln(color.Output, pre, color.BlueString(sql))
	} else if strings.HasPrefix(sql, "DELETE") {
		fmt.Fprintln(color.Output, pre, color.RedString(sql))
	} else {
		fmt.Fprintln(color.Output, pre, sql)
	}
}
