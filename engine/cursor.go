package engine

type Cursor interface {
	Advance() error
}
