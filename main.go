package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/fullstorydev/hauser/config"
	"github.com/fullstorydev/hauser/pipeline"
	"github.com/fullstorydev/hauser/warehouse"
)

var (
	conf               *config.Config
	currentBackoffStep = uint(0)
	bundleFieldsMap    = warehouse.BundleFields()
)

const (
	// Maximum number of times Hauser will attempt to retry each request made to FullStory
	maxAttempts int = 3

	// Default duration Hauser will wait before retrying a 429 or 5xx response. If Retry-After is specified, uses that instead. Default arbitrarily set to 10s.
	defaultRetryAfterDuration time.Duration = time.Duration(10) * time.Second
)

func BackoffOnError(err error) bool {
	if err != nil {
		if currentBackoffStep == uint(conf.BackoffStepsMax) {
			log.Fatalf("Reached max retries; exiting")
		}
		dur := conf.Backoff.Duration * (1 << currentBackoffStep)
		log.Printf("Pausing; will retry operation in %s", dur)
		time.Sleep(dur)
		currentBackoffStep++
		return true
	}
	currentBackoffStep = 0
	return false
}

func main() {
	conffile := flag.String("c", "config.toml", "configuration file")
	flag.Parse()

	var err error
	if conf, err = config.Load(*conffile); err != nil {
		log.Fatal(err)
	}

	/*
		var database warehouse.Database
		var storage warehouse.Storage
		switch conf.Provider {
		case "aws":
			database = warehouse.NewRedshift(conf.Redshift)
			storage = warehouse.NewS3Storage(conf.S3)
		case "gcp":
			database = warehouse.NewBigQuery(conf.BigQuery)
			storage = warehouse.NewGCPStorage(conf.GCS)
		case "local":
			storage = warehouse.NewLocalDisk(conf.Local)
		case "":
			log.Fatal("Provider must be specified in configuration")
		default:
			log.Fatalf("Unsupported provider: %s", conf.Provider)
		}
	*/
	// If the final destination for the data is a database, then we need to make
	// sure that the schema is updated. If FullStory has changed the schema for
	// events, then we add columns
	/*
		if conf.StorageOnly {
			if err := storage.InitSyncFile(); err != nil {
				log.Fatal(err)
			}
		} else {
			if err := database.InitSyncTable(); err != nil {
				log.Fatal(err)
			}

			if err := database.SyncExportTableSchema(); err != nil {
				log.Fatal(err)
			}
		}
	*/
	for {
		/*
			if !conf.StorageOnly {
				synct, err = database.LastSyncPoint()
			else {
				synct, err = storage.LastSyncPoint()
			}

			if BackoffOnError(err) {
				continue
			}
		*/
		pipeline := pipeline.NewPipeline(conf)

		savedfiles := pipeline.Start()
		defer pipeline.Stop()

		for savedfile := range savedfiles {
			fmt.Println(savedfile)
			/*
				name := bundle.StorageName()
				storage.Save(savedfile, name)

				if !conf.StorageOnly {
					database.LoadFromStorage(storage, name)
					storage.Delete(name)
				}
				if conf.RemoveTemp {
					os.Remove(savedfile)
				}
			*/
		}
	}
}
