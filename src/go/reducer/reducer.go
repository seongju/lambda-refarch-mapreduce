package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type LambdaPayload struct {
	Bucket      string   `json:"bucket"`
	Keys        []string `json:"keys"`
	JobBucket   string   `json:"jobBucket"`
	JobID       string   `json:"jobId"`
	NumReducers int      `json:"nReducers"`
	StepID      int      `json:"stepId"`
	ReducerID   int      `json:"reducerId"`
}

type Metadata struct {
	LineCount      int     `json:"linecount"`
	ProcessingTime float64 `json:"processingtime"`
}

func writeToS3(sess *session.Session, bucket, key string, data []byte, metadata map[string]*string) error {
	reader := bytes.NewReader(data)
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:     reader,
		Bucket:   &bucket,
		Key:      &key,
		Metadata: metadata,
	})
	return err
}

func handler(event LambdaPayload) ([]string, error) {
	const TASK_MAPPER_PREFIX string = "task/mapper/"
	const TASK_REDUCER_PREFIX string = "task/reducer/"
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3Client := s3.New(sess)

	startTime := float64(time.Now().UnixNano()) * math.Pow10(-9)

	var lineCount int
	output := make(map[string]float64)

	for _, object := range event.Keys {
		getObjectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: &event.Bucket,
			Key:    &object,
		})
		if err != nil {
			panic(err)
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(getObjectOutput.Body)
		contents := make(map[string]float64)

		err = json.Unmarshal(buf.Bytes(), &contents)
		if err != nil {
			panic(err)
		}

		for key, value := range contents {
			lineCount += 1
			if _, ok := output[key]; !ok {
				output[key] = 0.0
			}
			output[key] = output[key] + value
		}
	}

	timeInSecs := float64(time.Now().UnixNano())*math.Pow10(-9) - startTime
	lineCountString := strconv.Itoa(lineCount)
	processingTimeString := strconv.FormatFloat(timeInSecs, 'f', -1, 64)
	pret := []string{strconv.Itoa(len(event.Keys)), lineCountString, processingTimeString}
	fmt.Println("Reducer output: ", pret)

	var fileName string
	if event.NumReducers == 1 {
		fileName = fmt.Sprintf("%s/result", event.JobID)
	} else {
		fileName = fmt.Sprintf("%s/%s%s/%s", event.JobID, TASK_REDUCER_PREFIX, strconv.Itoa(event.StepID), strconv.Itoa(event.ReducerID))
	}

	var metadata = map[string]*string{
		"linecount":      &lineCountString,
		"processingtime": &processingTimeString,
		// TODO resource.getrusage equivalent for go? "memoryUsage":
	}
	outputJSON, err := json.Marshal(output)
	writeToS3(sess, event.JobBucket, fileName, outputJSON, metadata)
	return pret, err
}

func main() {
	lambda.Start(handler)
}
