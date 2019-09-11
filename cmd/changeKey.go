/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"encoding/base64"

	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"
)

var (
	changeKeyNewKeyAsBas64 string
	changeKeyWeakMode      bool
)

// changeKeyCmd represents the changeKey command
var changeKeyCmd = &cobra.Command{
	Use:   "change-key",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if changeKeyNewKeyAsBas64 == "" {
			log.Errorf("The new key must be provided\n")
			return
		}

		if changeKeyWeakMode {
			log.Infoln("Running in weak mode")
		}

		hadIssue := false
		parsedNewKey, err := base64.StdEncoding.DecodeString(changeKeyNewKeyAsBas64)
		if err != nil {
			log.Errorln("Bad given key, can't decode base64:", err.Error())
			hadIssue = true
		} else if len(parsedNewKey) != 32 {
			log.Errorln("Bad given key, the key should be 32 bytes long but is:", len(parsedNewKey))
			hadIssue = true
		}

		if hadIssue {
			if !changeKeyWeakMode {
				return
			}
			log.Warningln("Bad given key but continue in weak mode with the key:", parsedNewKey)
		}

		newKey := [32]byte{}
		copy(newKey[:], parsedNewKey)

		db, err := openDB(cmd, false)
		if err != nil {
			return
		}
		defer db.Close()

		err = db.UpdateKey(newKey)
		if err != nil {
			log.Errorf("Can't update the master key: %s\n", err.Error())
			return
		}
	},
}

func init() {
	changeKeyCmd.Flags().StringVarP(&changeKeyNewKeyAsBas64, "new-key", "n", "", "Defines the new key to use (required)")
	changeKeyCmd.Flags().BoolVar(&changeKeyWeakMode, "weak", false, "Returns no error if the the password is not a 32 bytes array")

	rootCmd.AddCommand(changeKeyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// changeKeyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// changeKeyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
