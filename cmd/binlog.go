package cmd

import (
	"context"
	"fmt"
	"os"

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
	if _, ok := binlogNameSizeM[conf.StartFile]; !ok {
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
	return binLog, nil
}

type BinLog struct {
	conf     *RootFlag
	eofFile  string
	eofPos   int
	serverId uint32
}

func (b *BinLog) Run() {
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
	streamer, _ := syncer.StartSync(mysql.Position{Name: b.conf.StartFile, Pos: b.conf.StartPosition})
	for {
		ev, _ := streamer.GetEvent(context.Background())
		if event, ok := ev.Event.(*replication.QueryEvent); ok {
			if string(event.Query) == "BEGIN" {
			}
			ev.Dump(os.Stdout)
		} else if event, ok := ev.Event.(*replication.RowsEvent); ok {
			_ = event
			ev.Dump(os.Stdout)
		} else {
			ev.Dump(os.Stdout)
		}
	}
}
