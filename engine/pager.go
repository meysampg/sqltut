package engine

type Pager interface {
	GetPage(pageNum uint32) ([]byte, error)
	Flush(pageNum int, size uint32) error
	GetNumPages() uint32
}
