package utils

import (
	"encoding/binary"
	"testing"

	"github.com/meysampg/sqltut/engine"
)

func TestSerialize(t *testing.T) {
	type args struct {
		row *engine.Row
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "bytes slice size equal to row size",
			args: args{
				row: &engine.Row{
					Id:       4,
					Username: "meysampg",
					Email:    "myemail@domain.com",
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Serialize(binary.BigEndian, tt.args.row)
			size := NewSize(tt.args.row)
			if len(got) != int(size.RowSize) {
				t.Errorf("Serialized size = %v, want %v", len(got), size.RowSize)
			}
		})
	}
}

func TestDeserialize(t *testing.T) {
	tests := []struct {
		name string
		want *engine.Row
	}{
		{
			name: "Serdes works properly",
			want: &engine.Row{
				Id:       5,
				Username: "meysampg",
				Email:    "myemail@domain.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Deserialize(binary.BigEndian, Serialize(binary.BigEndian, tt.want)); got.String() != tt.want.String() {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
