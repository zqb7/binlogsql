package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

type RootFlag struct {
	Host       string
	Port       uint16
	User       string
	Password   string
	DbName     string
	OnlyTables []string

	StartFile     string
	StartPosition uint32
	StopFile      string
	StopPosition  int

	_StartDateTimeStr string
	_StopDateTimeStr  string
	StartDateTime     time.Time
	StopDateTime      time.Time
	Flashback         bool
	StopNever         bool
}

func (rf *RootFlag) verify() error {
	if rf._StartDateTimeStr != "" {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", rf._StartDateTimeStr, time.Local)
		if err != nil {
			return err
		}
		rf.StartDateTime = t
	}

	if rf._StopDateTimeStr != "" {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", rf._StopDateTimeStr, time.Local)
		if err != nil {
			return err
		}
		rf.StopDateTime = t
	}

	return nil
}

var rootCmd = &cobra.Command{
	Use:   "binlogsql",
	Short: "binlogsql",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := rootFlag.verify(); err != nil {
			return err
		}
		binLog, err := NewBinLog(&rootFlag)
		if err != nil {
			return err
		}

		return binLog.Run()
	},
}

var rootFlag = RootFlag{}

func init() {
	rootCmd.PersistentFlags().Bool("help", false, "")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.Host, "host", "h", "127.0.0.1", "")
	rootCmd.PersistentFlags().Uint16VarP(&rootFlag.Port, "port", "P", 3306, "")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.User, "user", "u", "", "")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.Password, "password", "p", "", "")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.DbName, "database", "d", "", "")
	rootCmd.PersistentFlags().StringSliceVarP(&rootFlag.OnlyTables, "tables", "t", []string{}, "仅输出指定的表语句 ex:-t table1,table2")

	rootCmd.PersistentFlags().StringVar(&rootFlag.StartFile, "start-file", "", "")
	rootCmd.PersistentFlags().Uint32Var(&rootFlag.StartPosition, "start-pos", 0, "")
	rootCmd.PersistentFlags().StringVar(&rootFlag.StopFile, "stop-file", "", "")
	rootCmd.PersistentFlags().IntVar(&rootFlag.StopPosition, "end-pos", 0, "")
	rootCmd.PersistentFlags().StringVar(&rootFlag._StartDateTimeStr, "start-datetime", "", "起始解析时间(可选) 格式:%Y-%m-%d %H:%M:%S 例子: 2022-08-11 16:00:00")
	rootCmd.PersistentFlags().StringVar(&rootFlag._StopDateTimeStr, "stop-datetime", "", "截止解析时间(可选) 格式:%Y-%m-%d %H:%M:%S 例子: 2022-08-13 16:00:00")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.Flashback, "flashback", "", false, "")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.StopNever, "stop-never", "", false, "是否一直保持解析")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
