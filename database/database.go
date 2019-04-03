package database

import (
	"io"
)

type Bundle struct {
	id  int
	src *io.Reader
}

type Database interface {
	SyncExportTableSchema() error
	LastSyncPoint()
	Save(b *Bundle) error
}
