package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"io/ioutil"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	lambdaclient "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seongju/lambda-refarch-mapreduce/src/go/lambdautils"
)

type JobInfo struct {
	JobID             string `json:"jobId"`
	JobBucket         string `json:"jobBucket"`
	ReducerLambdaName string `json:"reducerFunction"`
	ReducerHandler    string `json:"reducerHandler"`
	MapCount          int    `json:"mapCount"`
}

type InvokeLambdaResult struct {
	Payload string
	Error   error
}

type LambdaPayload struct {
	Bucket      string   `json:"bucket"`
	Keys        []string `json:"keys"`
	JobBucket   string   `json:"jobBucket"`
	JobID       string   `json:"jobId"`
	NumReducers int      `json:"nReducers"`
	StepID      int      `json:"stepId"`
	ReducerID   int      `json:"reducerId"`
}

type ReducerState struct {
	ReducerCount int     `json:"reducerCount"`
	TotalS3Files int     `json:"totalS3Files"`
	StartTime    float64 `json:"start_time"`
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

func writeReducerState(sess *session.Session, numReducers, numS3 int, bucket, fileName string) error {
	startTime := float64(time.Now().UnixNano()) * math.Pow10(-9)
	data, err := json.Marshal(ReducerState{
		ReducerCount: numReducers,
		TotalS3Files: numS3,
		StartTime:    startTime,
	})
	if err != nil {
		return err
	}
	metadata := make(map[string]*string)
	err = writeToS3(sess, bucket, fileName, data, metadata)
	return err
}

func getMapperFiles(objects []*s3.Object) []*s3.Object {
	var mapperFiles []*s3.Object
	for _, object := range objects {
		file := *object.Key
		if strings.Contains(file, "task/mapper") {
			mapperFiles = append(mapperFiles, object)
		}
	}
	return mapperFiles
}

func getReducerBatchSize(objects []*s3.Object) int {
	batchSize := lambdautils.ComputeBatchSize(objects, 1536)
	if batchSize > 2 {
		return batchSize
	}
	return 2
}

func checkJobDone(objects []*s3.Object) bool {
	for _, object := range objects {
		if strings.Contains(*object.Key, "result") {
			return true
		}
	}
	return false
}

func getReducerStateInfo(s3Client *s3.S3, objects []*s3.Object, jobID, jobBucket string) (int, []*s3.Object) {
	var reducers []*s3.Object
	var reducerStep bool
	var rIndex int

	// Check for Reducer state
	// Determine the latest reducer step
	for _, object := range objects {
		objectKey := *object.Key
		if strings.Contains(objectKey, "reducerstate.") {
			idxString := strings.Split(objectKey, ".")[1]
			idx, err := strconv.Atoi(idxString)
			if err != nil {
				panic(err)
			}
			if idx > rIndex {
				rIndex = idx
			}
			reducerStep = true
		}
	}

	if !reducerStep {
		return 0, getMapperFiles(objects)
	} else {
		key := fmt.Sprintf("%s/reducerstate.%s", jobID, strconv.Itoa(rIndex))
		getObjectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: &jobBucket,
			Key:    &key,
		})
		if err != nil {
			panic(err)
		}

		var reducerState ReducerState
		buf := new(bytes.Buffer)
		buf.ReadFrom(getObjectOutput.Body)
		err = json.Unmarshal(buf.Bytes(), &reducerState)
		if err != nil {
			panic(err)
		}

		for _, object := range objects {
			objectKey := *object.Key
			splitObjectName := strings.Split(objectKey, "/")
			if len(splitObjectName) < 3 {
				continue
			}
			reducerObjectName := "reducer" + strconv.Itoa(rIndex)
			if strings.Contains(objectKey, reducerObjectName) {
				reducers = append(reducers, object)
			}
		}

		return rIndex, reducers
	}
}

func invokeLambda(lambdaClient *lambdaclient.Lambda, batch []string, reducerID, numReducers, stepID int, reducerLambdaName *string, bucket, jobBucket, jobID string, c chan InvokeLambdaResult) {
	var result string
	payload, err := json.Marshal(LambdaPayload{
		Bucket:      bucket,
		Keys:        batch,
		JobBucket:   jobBucket,
		JobID:       jobID,
		NumReducers: numReducers,
		StepID:      stepID,
		ReducerID:   reducerID,
	})

	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	invokeInput := &lambdaclient.InvokeInput{
		FunctionName: reducerLambdaName,
		Payload:      payload,
	}

	invokeOutput, err := lambdaClient.Invoke(invokeInput)
	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	result = string(invokeOutput.Payload)
	fmt.Println(result)
	c <- InvokeLambdaResult{result, nil}
	return
}

func handler(s3Event events.S3Event) {
	fmt.Println("Received event: ", s3Event)
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3Client := s3.New(sess)
	lambdaClient := lambdaclient.New(sess)
	bucket := s3Event.Records[0].S3.Bucket.Name

	raw, err := ioutil.ReadFile("./jobinfo.json")
	if err != nil {
		panic(err)
	}
	var config JobInfo
	err = json.Unmarshal(raw, &config)
	if err != nil {
		panic(err)
	}

	var maxKeys int64 = 1000
	listObjectsInput := &s3.ListObjectsInput{
		Bucket:  &bucket,
		Prefix:  &config.JobID,
		MaxKeys: &maxKeys,
	}

	listObjectsOutput, err := s3Client.ListObjects(listObjectsInput)
	if err != nil {
		panic(err)
	}
	allObjects := listObjectsOutput.Contents

	if checkJobDone(allObjects) == true {
		fmt.Println("Job done!!! Check the result file")
		return
	} else {
		if config.MapCount == len(getMapperFiles(allObjects)) {
			stepNumber, reducerKeys := getReducerStateInfo(s3Client, allObjects, config.JobID, bucket)

			if len(reducerKeys) == 0 {
				fmt.Println("Still waiting to finish the Reducer step ", stepNumber)
				return
			}

			reducerBatchSize := getReducerBatchSize(reducerKeys)

			fmt.Println("Starting the reducer step", stepNumber)
			fmt.Println("Batch size", reducerBatchSize)

			reducerBatches := lambdautils.BatchCreator(reducerKeys, reducerBatchSize)

			numReducers := len(reducerBatches)
			numS3 := numReducers * len(reducerBatches[0])
			stepID := stepNumber + 1

			resultChannel := make(chan InvokeLambdaResult, numReducers)
			for i := 0; i < numReducers; i +=1 {
				go invokeLambda(lambdaClient, reducerBatches[i], i, numReducers, stepID, &config.ReducerLambdaName, bucket, bucket, config.JobID, resultChannel)
			}
			for i := 0; i < numReducers; i +=1 {
				result := <-resultChannel
				if result.Error != nil {
					panic(result.Error)
				}
				fmt.Println(result.Payload)
			}

			fileName := fmt.Sprintf("%s/reducerstate.%s", config.JobID, strconv.Itoa(stepID))
			writeReducerState(sess, numReducers, numS3, bucket, fileName)
		} else {
			fmt.Println("Still waiting for all the mappers to finish")
		}
	}

}

func main() {
	lambda.Start(handler)
}
