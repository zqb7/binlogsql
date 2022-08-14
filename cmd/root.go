package cmd

import (
	"fmt"
	"os"

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
	StartDateTime string
	StopDateTime  string
	Flashback     bool
}

var rootCmd = &cobra.Command{
	Use:   "binlogsql",
	Short: "binlogsql",
	RunE: func(cmd *cobra.Command, args []string) error {
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
	rootCmd.PersistentFlags().StringVar(&rootFlag.StartDateTime, "start-datetime", "", "")
	rootCmd.PersistentFlags().StringVar(&rootFlag.StopDateTime, "stop-datetime", "", "")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.Flashback, "flashback", "", false, "")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
