package btree

import (
	"fmt"
	"io"
	"os"
)

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pages          [][]byte
	numPages       uint32
}

func NewPager(filename string) (*Pager, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fileLength, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if uint32(fileLength)%PageSize != 0 {
		return nil, fmt.Errorf("Db file is not a whole number of pages. Corrupt file.")
	}

	return &Pager{
		fileDescriptor: fd,
		fileLength:     uint32(fileLength),
		pages:          make([][]byte, TableMaxPage, TableMaxPage),
		numPages:       uint32(fileLength) / PageSize,
	}, nil
}

func (p *Pager) GetNumPages() uint32 {
	return p.numPages
}

func (p *Pager) GetPage(pageNum uint32) ([]byte, error) {
	if pageNum >= p.GetNumPages() {
		p.numPages = pageNum + 1
	}

	// Here we have cache miss; fetch from file
	if p.pages[pageNum] == nil {
		page := make([]byte, PageSize)
		numPages := p.fileLength / PageSize
		if p.fileLength%PageSize != 0 {
			// we have partial page saved on disk
			numPages++
		}

		// if we already have page on disk, will try to load it. Otherwise, we don't have this page and can skip this step.
		if pageNum < numPages {
			_, err := p.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
			if err != nil {
				return nil, err
			}
			n, err := p.fileDescriptor.Read(page)
			if n < 0 || err != nil {
				return nil, fmt.Errorf("Error reading file: %d", n) // f*ck the errno :))
			}
		}

		p.pages[pageNum] = page
	}

	return p.pages[pageNum], nil
}

func (p *Pager) Flush(pageNum int, size uint32) error {
	if p.pages[pageNum] == nil {
		return fmt.Errorf("Tried to flush null page")
	}

	if ret, err := p.fileDescriptor.Seek(int64(pageNum*int(PageSize)), io.SeekStart); err != nil || ret < 0 {
		return fmt.Errorf("Error seeking: %d", ret)
	}

	if n, err := p.fileDescriptor.Write(p.pages[pageNum][:PageSize]); n < 0 || err != nil {
		return fmt.Errorf("Error writing: %d", n)
	}

	return nil
}
