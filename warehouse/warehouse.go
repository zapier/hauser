package warehouse

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/nishanths/fullstory"
)

// Record represents a single export row in the export file
type Record map[string]interface{}

type Warehouse interface {
	LastSyncPoint() (time.Time, error)
	SaveSyncPoints(bundles ...fullstory.ExportMeta) error
	LoadToWarehouse(filename string, bundles ...fullstory.ExportMeta) error
	ValueToString(val interface{}, isTime bool) string
	GetExportTableColumns() []string
	EnsureCompatibleExportTable() error
	UploadFile(name string) (string, error)
	DeleteFile(path string)
	GetUploadFailedMsg(filename string, err error) string
	IsUploadOnly() bool
}

type Storage interface {
	Save(src *io.Reader, name string) error
	Delete(name string)
}

type Database interface {
	CreateSyncTable() error
	CreateExportTable() error
	SyncExportTableSchema() error
	LastSyncPoint() (time.Time, error)
	SaveSyncPoints(bundles ...fullstory.ExportMeta) error
	DeleteOrphanedRecords(t time.Time) (int, error)
	LoadFromStorage(ref string, s *Storage) error
	CleanRecord(rec map[string]interface{}) []string
}

// valueToString is a common interface method that implementations use to perform value to string conversion
func valueToString(val interface{}, isTime bool) string {
	s := fmt.Sprintf("%v", val)
	if isTime {
		t, _ := time.Parse(time.RFC3339Nano, s)
		return t.Format(time.RFC3339Nano)
	}

	s = strings.Replace(s, "\n", " ", -1)
	s = strings.Replace(s, "\r", " ", -1)
	s = strings.Replace(s, "\x00", "", -1)

	return s
}
