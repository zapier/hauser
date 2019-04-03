package pipeline

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"

	"github.com/fullstorydev/hauser/warehouse"
	"github.com/nishanths/fullstory"
)

type ExportData struct {
	meta fullstory.ExportMeta
	src  io.Reader
}

func (d *ExportData) GetNextRecord() warehouse.Record {

}

func (d *ExportData) GetCSVReader(headers []string) (io.ReadCloser, error) {
	stream, err := gzip.NewReader(d.src)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	piper, pipew := io.Pipe()
	csvWriter := csv.NewWriter(pipew)

	go func() {
		decoder := json.NewDecoder(stream)
		decoder.UseNumber()

		// skip array open delimiter
		if _, err := decoder.Token(); err != nil {
			log.Printf("Failed json decode of array open token: %s", err)
			return
		}

		csvWriter.Write(headers)

		for decoder.More() {
			var r warehouse.Record
			decoder.Decode(&r)
			csvWriter.Write(r.GetValueStrings(headers))
		}

		if _, err := decoder.Token(); err != nil {
			log.Fatalf("Failed json decode of array close token: %s", err)
		}
		csvWriter.Flush()
		pipew.Close()
	}()

	return piper, nil
}

func (d *ExportData) GetRawReader() (io.ReadCloser, error) {
	return gzip.NewReader(d.src)
}

func (d *ExportData) GetReader(format string, headers []string) (io.ReadCloser, error) {
	switch format {
	case "csv":
		return d.GetCSVReader(headers)
	default:
		return d.GetRawReader()
	}
}
