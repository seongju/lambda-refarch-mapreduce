package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type LambdaPayload struct {
	Bucket    string   `json:"bucket"`
	Keys      []string `json:"keys"`
	JobBucket string   `json:"jobBucket"`
	JobID     string   `json:"jobId"`
	MapperID  int      `json:"mapperId"`
}

type Metadata struct {
	LineCount int `json:"linecount"`
	ProcessingTime float64 `json:"processingtime"`
}

func writeToS3(sess *session.Session, bucket, key string, data []byte, metadata map[string]*string) error {
	reader := bytes.NewReader(data)
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: &bucket,
		Key:    &key,
		Metadata: metadata,
	})
	return err
}

func handler(event LambdaPayload) ([]string, error) {
	const TASK_MAPPER_PREFIX string = "task/mapper/"
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3Client := s3.New(sess)
	startTime := float64(time.Now().UnixNano()) * math.Pow10(-9)

	var lineCount int
	output := make(map[string]float64)
	
	for _, object := range(event.Keys) {
		getObjectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: &event.Bucket,
			Key: &object,
		})
		if err != nil {
			panic(err)
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(getObjectOutput.Body)
		lines := strings.Split(buf.String(), "\n")

		for i := 0; i < len(lines) -1; i++ {
			line := lines[i]
			lineCount += 1
			data := strings.Split(line, ",")
			var srcIp string
			if len(data[0]) > 8 {
				srcIp = data[0][:8]
			} else {
				srcIp = data[0]
			}
			count, err:= strconv.ParseFloat(data[3], 64)
			if err != nil {
				panic(err)
			}
			if _, ok := output[srcIp]; !ok {
				output[srcIp] = 0.0
			}
			output[srcIp] = output[srcIp] + count
		}
	}
	timeInSecs := float64(time.Now().UnixNano()) * math.Pow10(-9) - startTime
	lineCountString := strconv.Itoa(lineCount)
	processingTimeString := strconv.FormatFloat(timeInSecs, 'f', -1, 64)
	pret := []string{strconv.Itoa(len(event.Keys)), lineCountString, processingTimeString}
	mapperFileName := fmt.Sprintf("%s/%s%s", event.JobID, TASK_MAPPER_PREFIX, strconv.Itoa(event.MapperID))
	var metadata = map[string]*string{
		"linecount": &lineCountString,
		"processingtime": &processingTimeString,
		// TODO resource.getrusage equivalent for go? "memoryUsage": 
	}
	fmt.Println("metadata: ", metadata)
	outputJSON, err := json.Marshal(output)
	writeToS3(sess, event.JobBucket, mapperFileName, outputJSON, metadata)
	return pret, err
}

func main() {
	lambda.Start(handler)
}
