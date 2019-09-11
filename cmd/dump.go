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
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"
)

var (
	dumpTarget     string
	dumpJSON       bool
	dumpJSONPretty bool
	dumpJSONFile   bool
)

type (
	Dump struct {
		Collections []*Collection
		Files       []*File `json:",omitempty"`
	}
	Collection struct {
		Name    string
		Records []*Record
	}
	Record struct {
		ID         string
		Content    json.RawMessage `json:",omitempty"`
		RawContent []byte          `json:",omitempty"`
	}
	File struct {
		ID                        string
		Name                      string
		Size                      int64
		LastModified              time.Time
		RelatedDocumentID         string
		RelatedDocumentCollection string

		Content []byte
	}
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Open the database and dump it's content into an archive",
	Long: `Open the database and dump it's content into an archive. You can build full archive or a JSON archive which has actual stat but no history.

The JSON format decrypt the data and make it readable and exportable to other systems.

In comparison the binary export is fully encrypted and keeps everything (history, indexes files and all metas).
To use this dump you need to inject the backup in a database using the same encryption key as the one used for the "dump".`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := openDB(cmd, true)
		if err != nil {
			return
		}
		defer db.Close()

		destFile, err := os.OpenFile(dumpTarget, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Errorln("Can't open dump target:", err.Error())
			return
		}

		if !dumpJSON {
			err = db.Backup(destFile)
			if err != nil {
				log.Errorln("Can't backup database:", err.Error())
				return
			}
		} else {
			ret := new(Dump)
			ret.Collections = []*Collection{}
			for _, col := range db.Collections {
				col, err := db.Use(col.GetName())
				if err != nil {
					log.Warningf("err opening collection %q: %s\n", col.GetName(), err.Error())
					continue
				}

				dumpCol := new(Collection)
				dumpCol.Name = col.GetName()
				dumpCol.Records = []*Record{}
				ret.Collections = append(ret.Collections, dumpCol)

				iter := col.GetIterator()
				for ; iter.Valid(); iter.Next() {
					rec := new(Record)
					rec.ID = iter.GetID()

					content := iter.GetBytes()

					// Make sure the content is a JSON.
					// Otherways it's send to RawContent.
					err := json.Unmarshal(content, new(struct{}))
					if err == nil {
						rec.Content = content
					} else {
						rec.RawContent = content
					}

					dumpCol.Records = append(dumpCol.Records, rec)
				}
				iter.Close()
			}

			if dumpJSONFile {
				ret.Files = []*File{}
				iter := db.FileStore.GetFileIterator()
				for ; iter.Valid(); iter.Next() {
					meta := iter.GetMeta()
					dumpFile := new(File)
					dumpFile.ID = meta.ID
					dumpFile.Name = meta.Name
					dumpFile.Size = meta.Size
					dumpFile.LastModified = meta.LastModified
					dumpFile.RelatedDocumentID = meta.RelatedDocumentID
					dumpFile.RelatedDocumentCollection = meta.RelatedDocumentCollection

					reader, err := db.FileStore.GetFileReader(meta.ID)
					if err != nil {
						log.Warningln("err opening file:", err.Error())
						continue
					}

					buff, err := ioutil.ReadAll(reader)
					if err != nil {
						log.Warningln("err reading file:", err.Error())
						continue
					}
					dumpFile.Content = buff

					ret.Files = append(ret.Files, dumpFile)
				}
				iter.Close()
			}

			var buff []byte
			if !dumpJSONPretty {
				buff, err = json.Marshal(ret)
			} else {
				buff, err = json.MarshalIndent(ret, "", "	")
			}
			if err != nil {
				log.Errorln("err marshaling dump:", err.Error())
				return
			}

			_, err = destFile.Write(buff)
			if err != nil {
				log.Errorln("err writing JSON dump:", err.Error())
				return
			}
		}
	},
}

func init() {
	dumpCmd.Flags().StringVarP(&dumpTarget, "target", "t", "./db-archive", "Defines the dump destination")
	dumpCmd.Flags().BoolVar(&dumpJSON, "json", false, "Saves a JSON content instead of the encrypted stream. This can consume lots of memory to keep all records of all collections to build the output JSON.")
	dumpCmd.Flags().BoolVar(&dumpJSONPretty, "pretty", false, "Needs --json to work. It returns the JSON in a readable form")
	dumpCmd.Flags().BoolVar(&dumpJSONFile, "files", false, "Needs --json to work. Add files to output")

	rootCmd.AddCommand(dumpCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dumpCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dumpCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
