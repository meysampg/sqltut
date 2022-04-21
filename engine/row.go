package engine

import "fmt"

type Row struct {
	Id       uint32
	Username string
	Email    string
}

func (r *Row) String() string {
	return fmt.Sprintf("(%d, %s, %s)", r.Id, r.Username, r.Email)
}
