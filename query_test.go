package gotinydb

import (
	"reflect"
	"testing"
)

func TestCollection_Query(t *testing.T) {
	type args struct {
		q *Query
	}
	tests := []struct {
		name         string
		c            *Collection
		args         args
		wantResponse *Response
		wantErr      bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := tt.c.Query(tt.args.q)
			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResponse, tt.wantResponse) {
				t.Errorf("Collection.Query() = %v, want %v", gotResponse, tt.wantResponse)
			}
		})
	}
}
