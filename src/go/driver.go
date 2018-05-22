package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seongju/lambda-refarch-mapreduce/src/go/lambdautils"
)

type JobData struct {
	MapCount     int     `json:"n_mapper"`
	TotalS3Files int     `json:"totalS3Files"`
	StartTime    float64 `json:"startTime"`
}

type LambdaFunction struct {
	Path    string `json:"path"`
	Bin string `json:"bin"`
	Zip     string `json:"zip"`
}

type ConfigFile struct {
	Bucket             string         `json:"bucket"`
	Prefix             string         `json:"prefix"`
	JobBucket          string         `json:"jobBucket"`
	Region             string         `json:"region"`
	LambdaMemory       int            `json:"lambdaMemory"`
	ConcurrentLambdas  int            `json:"concurrentLambdas"`
	Mapper             LambdaFunction `json:"mapper"`
	Reducer            LambdaFunction `json:"reducer"`
	ReducerCoordinator LambdaFunction `json:"reducerCoordinator"`
}

type JobInfo struct {
	JobID             string `json:"jobId"`
	JobBucket         string `json:"jobBucket"`
	ReducerLambdaName string `json:"reducerFunction"`
	ReducerHandler    string `json:"reducerHandler"`
	MapCount          int    `json:"mapCount"`
}

type LambdaPayload struct {
	Bucket    string   `json:"bucket"`
	Keys      []string `json:"keys"`
	JobBucket string   `json:"jobBucket"`
	JobID     string   `json:"jobId"`
	MapperID  int      `json:"mapperId"`
}

type InvokeLambdaResult struct {
	Payload []string
	Error   error
}

const JobInfoFile = "jobinfo.json"

func writeJobConfig(jobID, jobBucket, reducerLambdaName, reducerHandler string, mapCount int) error {
	fileName := JobInfoFile
	jobInfo := JobInfo{
		jobID,
		jobBucket,
		reducerLambdaName,
		reducerHandler,
		mapCount,
	}
	jobInfoJSON, err := json.Marshal(jobInfo)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fileName, jobInfoJSON, 0644)
	return err
}

func writeToS3(sess *session.Session, bucket, key string, data []byte) error {
	reader := bytes.NewReader(data)
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: &bucket,
		Key:    &key,
	})
	return err
}

// crossCompile builds lambdaFileName as a binary with binName
// as the name. It returns the location of a built binary file.
// Heavily influenced from bcongdon/corral's crossCompile func
func createBinary(binName, lambdaFilePath string) (string, error) {
	args := []string{
		"build",
		"-o", binName,
		lambdaFilePath,
	}
	cmd := exec.Command("go", args...)

	cmd.Env = append(os.Environ(), "GOOS=linux")

	combinedOut, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s\n%s", err, combinedOut)
	}

	return binName, nil
}

// createLambdaPackage zips up jobInfo.json and the lambda bin.
// Heavily influenced from bcongdon/corral's buildPackage func
func createLambdaPackage(binName, lambdaFilePath, zipName string) error {
	binPath, err := createBinary(binName, lambdaFilePath)
	if err != nil {
		return err
	}

	zipBuf, err := os.Create(zipName) 
	if err != nil {
		return err
	}
	zipWriter := zip.NewWriter(zipBuf)

	binHeader := &zip.FileHeader{
		Name: binName,
		ExternalAttrs:  (0777 << 16), // File permissions
		CreatorVersion: (3 << 8),     // Magic number indicating a Unix creator
	}
	binReader, err := os.Open(binPath)
	if err != nil {
		return err
	}

	binWriter, err := zipWriter.CreateHeader(binHeader)
	if err != nil {
		return err
	}

	_, err = io.Copy(binWriter, binReader)
	if err != nil {
		return err
	}
	binReader.Close()

	jobInfoHeader := &zip.FileHeader{
		Name: JobInfoFile,
		ExternalAttrs:  (0777 << 16), 
		CreatorVersion: (3 << 8),
	}
	jobInfoReader, err := os.Open(JobInfoFile)
	if err != nil {
		return err
	}
	jobInfoWriter, err := zipWriter.CreateHeader(jobInfoHeader)
	if err != nil {
		return err
	}
	_, err = io.Copy(jobInfoWriter, jobInfoReader)
	if err != nil {
		return err
	}
	jobInfoReader.Close()

	err = zipWriter.Close()
	if err != nil {
		return err
	}
	return zipBuf.Close()
}

func invokeLambda(lambdaClient *lambda.Lambda, batch []string, mapperID int, mapperLambdaName *string, bucket, jobBucket, jobID string, c chan InvokeLambdaResult) {
	var result []string
	payload, err := json.Marshal(LambdaPayload{
		Bucket:    bucket,
		Keys:      batch,
		JobBucket: jobBucket,
		JobID:     jobID,
		MapperID:  mapperID,
	})

	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	invokeInput := &lambda.InvokeInput{
		FunctionName: mapperLambdaName,
		Payload:      payload,
	}

	invokeOutput, err := lambdaClient.Invoke(invokeInput)
	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	err = json.Unmarshal(invokeOutput.Payload, &result)
	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}
	fmt.Println(result)
	c <- InvokeLambdaResult{result, nil}
	return
}

func main() {
	//  JOB ID
	jobID := os.Args[1]
	fmt.Printf("Starting job %s\n", jobID)

	// Retrieve the values in driverconfig.json
	raw, err := ioutil.ReadFile("./driverconfig.json")
	if err != nil {
		panic(err)
	}
	var config ConfigFile
	err = json.Unmarshal(raw, &config)
	if err != nil {
		panic(err)
	}

	bucket := config.Bucket
	jobBucket := config.JobBucket
	region := config.Region
	lambdaMemory := config.LambdaMemory
	concurrentLambdas := config.ConcurrentLambdas

	fmt.Println(bucket)
	fmt.Println(jobBucket)
	fmt.Println(region)
	fmt.Println(lambdaMemory)
	fmt.Println(concurrentLambdas)

	// Fetch the keys that match the prefix from the config
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3Client := s3.New(sess)
	var maxKeys int64 = 1000
	listObjectsInput := &s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &config.Prefix,
		MaxKeys: &maxKeys,
	}
	listObjectsOutput, err := s3Client.ListObjects(listObjectsInput)
	if err != nil {
		panic(err)
	}
	allObjects := listObjectsOutput.Contents

	objectsPerBatch := lambdautils.ComputeBatchSize(allObjects, lambdaMemory)
	batches := lambdautils.BatchCreator(allObjects, objectsPerBatch)
	numMappers := len(batches)

	lambdaPrefix := "BL"
	mapperLambdaName := lambdaPrefix + "-mapper-" + jobID
	reducerLambdaName := lambdaPrefix + "-reducer-" + jobID
	reducerCoordinatorLambdaName := lambdaPrefix + "-reducerCoordinator-" + jobID

	err = writeJobConfig(jobID, jobBucket, reducerLambdaName, config.Reducer.Bin, numMappers)
	if err != nil {
		panic(err)
	}

	err = createLambdaPackage(config.Mapper.Bin, config.Mapper.Path, config.Mapper.Zip)
	if err != nil {
		panic(err)
	}
	err = createLambdaPackage(config.Reducer.Bin, config.Reducer.Path, config.Reducer.Zip)
	if err != nil {
		panic(err)
	}
	err = createLambdaPackage(config.ReducerCoordinator.Bin, config.ReducerCoordinator.Path, config.ReducerCoordinator.Zip)
	if err != nil {
		panic(err)
	}

	lambdaClient := lambda.New(sess)
	mapperLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.Mapper.Zip,
		JobID:        jobID,
		LambdaName:   mapperLambdaName,
		HandlerName:  config.Mapper.Bin,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 3000,
		Timeout:      300,
	}
	err = mapperLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Mapper Function ARN: %s\n", mapperLambdaManager.FunctionArn)
	defer mapperLambdaManager.DeleteLambda()

	reducerLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.Reducer.Zip,
		JobID:        jobID,
		LambdaName:   reducerLambdaName,
		HandlerName:  config.Reducer.Bin,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 3000,
		Timeout:      300,
	}
	err = reducerLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Reducer Function ARN: %s\n", reducerLambdaManager.FunctionArn)
	defer reducerLambdaManager.DeleteLambda()

	reducerCoordLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.ReducerCoordinator.Zip,
		JobID:        jobID,
		LambdaName:   reducerCoordinatorLambdaName,
		HandlerName:  config.ReducerCoordinator.Bin,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 3000,
		Timeout:      300,
	}

	err = reducerCoordLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Reducer Coordinator Function ARN: %s\n", reducerCoordLambdaManager.FunctionArn)
	defer reducerCoordLambdaManager.DeleteLambda()

	err = reducerCoordLambdaManager.AddLambdaPermission(string(rand.Intn(1000)), "arn:aws:s3:::"+jobBucket)
	if err != nil {
		if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 409 {
			fmt.Println("Statement already exists on Reducer Coordinator")
		} else {
			panic(err)
		}
	}

	err = reducerCoordLambdaManager.CreateS3EventSourceNotification(jobBucket, jobID)
	if err != nil {
		panic(err)
	}

	// Write job data to S3
	jobData := JobData{
		MapCount:     numMappers,
		TotalS3Files: len(allObjects),
		StartTime:    float64(time.Now().UnixNano()) * math.Pow10(-9),
	}
	jobDataJSON, err := json.Marshal(jobData)
	if err != nil {
		panic(err)
	}

	err = writeToS3(sess, jobBucket, jobID+"/jobdata", jobDataJSON)
	if err != nil {
		panic(err)
	}

	// TODO respect concurrentLambdas variable
	resultChannel := make(chan InvokeLambdaResult, numMappers)
	for mapperID := 0; mapperID < numMappers; mapperID += 1 {
		go invokeLambda(lambdaClient, batches[mapperID], mapperID, &mapperLambdaName, bucket, jobBucket, jobID, resultChannel)
	}

	var totalS3GetOps int
	var totalLines int
	var totalLambdaSecs float64

	for i := 1; i < numMappers + 1; i += 1 {
		result := <-resultChannel
		if result.Error != nil {
			panic(result.Error)
		}
		// TODO should check for length of 4 in result.Payload to see if the Lambda returned an error
		s3GetOps, err := strconv.Atoi(result.Payload[0])
		if err != nil {
			panic(err)
		}
		totalS3GetOps += s3GetOps
		numLines, err := strconv.Atoi(result.Payload[1])
		if err != nil {
			panic(err)
		}
		totalLines += numLines
		seconds, err := strconv.ParseFloat(result.Payload[2], 64)
		if err != nil {
			panic(err)
		}
		totalLambdaSecs += seconds
	}

	var totalS3Size int64
	var jobDone = false
	var reducerFileExists = false
	var keys []string
	var reducerLambdaTime float64
	resultFile := jobID + "/result"
	reducerFile := "task/reducer"

	for !jobDone {
		fmt.Println("Checking to see if job is done")
		listObjectsInput = &s3.ListObjectsInput{
			Bucket: &jobBucket,
			Prefix: &jobID,
		}
		listObjectsOutput, err = s3Client.ListObjects(listObjectsInput)
		if err != nil {
			panic(err)
		}
		allObjects = listObjectsOutput.Contents
		keys = []string{}
		for _, object := range allObjects {
			key := *object.Key
			if key == resultFile {
				jobDone = true
			} else if strings.Contains(key, reducerFile) {
				reducerFileExists = true
			}
			keys = append(keys, key)
			totalS3Size += *object.Size
		}
		time.Sleep(5 * time.Second)
	}

	fmt.Println("Job is done")
	headObjectInput := &s3.HeadObjectInput{
		Bucket: &jobBucket,
		Key:    &resultFile,
	}
	headObjectOutput, err := s3Client.HeadObject(headObjectInput)
	if err != nil {
		panic(err)
	}
	reducerLambdaTime, err = strconv.ParseFloat(*headObjectOutput.Metadata["Processingtime"], 64)

	if reducerFileExists {
		headObjectInput = &s3.HeadObjectInput{
			Bucket: &jobBucket,
			Key:    &reducerFile,
		}
		headObjectOutput, err := s3Client.HeadObject(headObjectInput)
		if err != nil {
			panic(err)
		}
		lambdaTime, err := strconv.ParseFloat(*headObjectOutput.Metadata["Processingtime"], 64)
		if err != nil {
			panic(err)
		}
		reducerLambdaTime += lambdaTime
	}

	// S3 Storage cost - Account for mappers only; This cost is neglibile anyways since S3
	// costs 3 cents/GB/month
	s3StorageHourCost := 0.0000521574022522109 * (float64(totalS3Size) / 1024.0 / 1024.0 / 1024.0) // cost per GB/hr
	s3PutCost := float64(len(allObjects)) * 0.005 / float64(1000)

	// S3 GET # $0.004/10000
	totalS3GetOps += len(allObjects)
	s3GetCost := float64(totalS3GetOps) * 0.004 / float64(10000)

	// Total Lambda costs
	totalLambdaSecs += reducerLambdaTime
	lambdaCost := float64(totalLambdaSecs) * 0.00001667 * float64(lambdaMemory) / 1024.0
	s3Cost := (s3GetCost + s3PutCost + s3StorageHourCost)
	fmt.Printf("Reducer Lambda Cost: %f\n", float64(reducerLambdaTime)*0.00001667*float64(lambdaMemory)/1024.0)
	fmt.Printf("Lambda Cost: %f\n", lambdaCost)
	fmt.Printf("S3 Storage Cost: %f\n", s3StorageHourCost)
	fmt.Printf("S3 Request Cost: %f\n", s3GetCost+s3PutCost)
	fmt.Printf("S3 Cost: %f\n", s3Cost)
	fmt.Printf("Total Cost: %f\n", lambdaCost+s3Cost)
	fmt.Printf("Total Lines: %d\n", totalLines)
}
