package gotinydb

import "encoding/json"

func (i *Index) parseIDs(idsAsBytes []byte) (ids []string, err error) {
	err = json.Unmarshal(idsAsBytes, &ids)
	return
}

func (i *Index) formatIDs(idsAsStrings []string) (ids []byte, err error) {
	return json.Marshal(idsAsStrings)
}
