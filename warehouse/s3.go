package warehouse

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fullstorydev/hauser/config"
	"github.com/nishanths/fullstory"
)

type S3Storage struct {
	conf   *config.S3Config
	format string
}

// NewS3Storage returns an S3Storage pointer that can be used to save data to an AWS S3 bucket
func NewS3Storage(c *config.S3Config) *S3Storage {
	return &S3Storage{
		conf: c,
	}
}

// Save ...
func (ss *S3Storage) Save(src io.ReadSeeker, name string) (string, error) {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess, aws.NewConfig().WithRegion(ss.conf.Region))

	ctx, cancel := context.WithTimeout(context.Background(), ss.conf.Timeout.Duration)
	defer cancel()

	bucketName, key := getBucketAndKey(ss.conf.Bucket, name)

	_, err := svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   src,
	})

	s3path := fmt.Sprintf("s3://%s/%s", bucketName, key)
	return s3path, err
}

func (ss *S3Storage) SaveSyncPoints(bundles ...fullstory.ExportMeta) error {
	return nil
}

func (ss *S3Storage) LastSyncPoint() {
	return
}

func (ss *S3Storage) Open(name, dest io.ReadSeeker) error {
	return nil
}

func (ss *S3Storage) Delete(name string) {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess, aws.NewConfig().WithRegion(ss.conf.Region))

	ctx, cancel := context.WithTimeout(context.Background(), ss.conf.Timeout.Duration)
	defer cancel()

	_, objName := filepath.Split(name)
	_, err := svc.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(ss.conf.Bucket),
		Key:    aws.String(objName),
	})
	if err != nil {
		log.Printf("failed to delete S3 object %s: %s", name, err)
		// just return - object will remain in S3
	}
}

func getBucketAndKey(bucketConfig, objName string) (string, string) {
	bucketParts := strings.Split(bucketConfig, "/")
	bucketName := bucketParts[0]
	keyPath := strings.Trim(strings.Join(bucketParts[1:], "/"), "/")
	key := strings.Trim(fmt.Sprintf("%s/%s", keyPath, objName), "/")

	return bucketName, key
}
