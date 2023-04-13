package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zqhhh/binlogsql"
)

var rootCmd = &cobra.Command{
	Use:   "binlogsql",
	Short: "binlogsql",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := rootFlag.Verify(); err != nil {
			return err
		}
		binLog, err := binlogsql.NewBinLog(&rootFlag)
		if err != nil {
			return err
		}
		return binLog.Run()
	},
}

var rootFlag = binlogsql.RootFlag{}

func init() {
	rootCmd.PersistentFlags().Bool("help", false, "")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.Host, "host", "h", "127.0.0.1", "主机地址")
	rootCmd.PersistentFlags().Uint16VarP(&rootFlag.Port, "port", "P", 3306, "端口号")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.User, "user", "u", "", "用户名")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.Password, "password", "p", "", "密码")
	rootCmd.PersistentFlags().StringVarP(&rootFlag.DbName, "database", "d", "", "解析指定的数据库")
	rootCmd.PersistentFlags().StringSliceVarP(&rootFlag.OnlyTables, "tables", "t", []string{}, "仅输出指定的表语句 ex:-t table1,table2")

	rootCmd.PersistentFlags().StringVar(&rootFlag.StartFile, "start-file", "", "默认为最新的文件 ex: --start-file mysql-bin.000002")
	rootCmd.PersistentFlags().Uint32Var(&rootFlag.StartPosition, "start-pos", 0, "默认为最新的位置 ex: --start-pos 154")
	rootCmd.PersistentFlags().StringVar(&rootFlag.EndFile, "end-file", "", "截止解析的文件 ex: --end-file mysql-bin.000003")
	rootCmd.PersistentFlags().IntVar(&rootFlag.EndPosition, "end-pos", 0, "截止解析的位置 ex: --end-pos 154")
	rootCmd.PersistentFlags().StringVar(&rootFlag.StartDateTimeStr, "start-datetime", "", "起始解析时间(可选) 格式:%Y-%m-%d %H:%M:%S ex: 2022-08-11 16:00:00")
	rootCmd.PersistentFlags().StringVar(&rootFlag.StopDateTimeStr, "stop-datetime", "", "截止解析时间(可选) 格式:%Y-%m-%d %H:%M:%S ex: 2022-08-13 16:00:00")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.Flashback, "flashback", "", false, "生成回滚语句")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.StopNever, "stop-never", "", false, "是否一直保持解析")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.SaveFile, "save", "", false, "是否写入sql到文件中")
	rootCmd.PersistentFlags().BoolVarP(&rootFlag.Quiet, "q", "", false, "安静模式,不输出到控制台")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
