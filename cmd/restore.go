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
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	restoreSource string
	restoreJSON   bool
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Load the database with the save content",
	Long: `This command opens the database in read/write mode (no other service running on it) to 
fill-up the database with the given archive (JSON or full archive)`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := openDB(cmd, false)
		if err != nil {
			return
		}
		defer db.Close()

		sourceFile, err := os.Open(restoreSource)
		if err != nil {
			log.Errorf("Can't open source file: %s", err.Error())
			return
		}

		if !restoreJSON {
			err = db.Load(sourceFile)
			if err != nil {
				log.Errorln("Can't restore database:", err.Error())
				return
			}
		} else {
			dumpObj := new(Dump)

			buff, err := ioutil.ReadAll(sourceFile)
			if err != nil {
				log.Errorln("Can't read source file:", err.Error())
				return
			}

			err = json.Unmarshal(buff, dumpObj)
			if err != nil {
				log.Errorln("Can't unmarshal JSON:", err.Error())
				return
			}

			for _, savedCol := range dumpObj.Collections {
				col, err := db.Use(savedCol.Name)
				if err != nil {
					log.Warningln("Can't open collection:", err.Error())
					continue
				}

				batch, err := col.NewBatch(context.Background())
				if err != nil {
					log.Warningln("Can't open collection batch:", err.Error())
					continue
				}

				for i, rec := range savedCol.Records {
					err := batch.Put(rec.ID, rec)
					if err != nil {
						log.Warningln("Can't put record into batch:", err.Error())
						continue
					}

					if i%1000 == 0 {
						err = batch.Write()
						if err != nil {
							log.Warningln("Can't write the batch:", err.Error())
							continue
						}
					}
				}
				err = batch.Write()
				if err != nil {
					log.Warningln("Can't write the batch:", err.Error())
					continue
				}
			}

			for _, savedFile := range dumpObj.Files {
				buff := bytes.NewBuffer(savedFile.Content)
				_, err = db.GetFileStore().PutFile(savedFile.ID, savedFile.Name, buff)
				if err != nil {
					log.Warningf("Can't put file %q with ID %q because: %s\n", savedFile.Name, savedFile.ID, err.Error())
				}
			}
		}
	},
}

func init() {
	restoreCmd.Flags().StringVarP(&restoreSource, "source", "s", "./db-archive", "Defines the restore source")
	restoreCmd.Flags().BoolVar(&restoreJSON, "json", false, "import a JSON content instead of the encrypted stream.")

	rootCmd.AddCommand(restoreCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// restoreCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// restoreCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
