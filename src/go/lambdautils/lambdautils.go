package lambdautils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
)

type LambdaManager struct {
	LambdaClient                                                         *lambda.Lambda
	S3Client                                                             *s3.S3
	Region, PathToZip, JobID, LambdaName, HandlerName, Role, FunctionArn string
	LambdaMemory, Timeout                                                int64
}

func (lm *LambdaManager) CreateLambda() error {
	runtime := "go1.x"
	createFunctionInput := &lambda.CreateFunctionInput{
		FunctionName: &lm.LambdaName,
		Handler:      &lm.HandlerName,
		Role:         &lm.Role,
		Runtime:      &runtime,
		MemorySize:   &lm.LambdaMemory,
		Timeout:      &lm.Timeout,
	}

	zipFileBytes, err := ioutil.ReadFile(lm.PathToZip)
	if err != nil {
		return err
	}

	functionCode := &lambda.FunctionCode{
		ZipFile: zipFileBytes,
	}
	createFunctionInput = createFunctionInput.SetCode(functionCode)

	functionConfig, err := lm.LambdaClient.CreateFunction(createFunctionInput)
	if err != nil {
		return err
	}
	lm.FunctionArn = *functionConfig.FunctionArn
	return nil
}

func (lm *LambdaManager) DeleteLambda() error {
	deleteFunctionInput := &lambda.DeleteFunctionInput{
		FunctionName: &lm.LambdaName,
	}
	_, err := lm.LambdaClient.DeleteFunction(deleteFunctionInput)
	return err
}

func (lm *LambdaManager) UpdateLambda() error {
	zipFileBytes, err := ioutil.ReadFile(lm.PathToZip)
	if err != nil {
		return err
	}

	updateFunctionCodeInput := &lambda.UpdateFunctionCodeInput{
		FunctionName: &lm.LambdaName,
		//Publish: true,
		ZipFile: zipFileBytes,
	}
	functionConfig, err := lm.LambdaClient.UpdateFunctionCode(updateFunctionCodeInput)
	if err != nil {
		return err
	}
	lm.FunctionArn = *functionConfig.FunctionArn
	return nil
}

func (lm *LambdaManager) CreateOrUpdateLambda() error {
	err := lm.CreateLambda()
	if err != nil {
		if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 409 {
			fmt.Println("Functions already exist, updating")
			err = lm.UpdateLambda()
		}
		return err
	}
	return nil
}

func (lm *LambdaManager) AddLambdaPermission(sid, bucketArn string) error {
	principal := "s3.amazonaws.com"
	action := "lambda:InvokeFunction"
	addPermissionInput := &lambda.AddPermissionInput{
		Action:       &action,
		FunctionName: &lm.LambdaName,
		Principal:    &principal,
		SourceArn:    &bucketArn,
		StatementId:  &sid,
	}
	addPermissionOutput, err := lm.LambdaClient.AddPermission(addPermissionInput)
	if err != nil {
		return err
	}
	fmt.Println(addPermissionOutput)
	return nil
}

func (lm *LambdaManager) CreateS3EventSourceNotification(bucket, prefix string) error {
	key := "prefix"
	value := prefix + "/task"
	rule := &s3.FilterRule{
		Name:  &key,
		Value: &value,
	}

	filterRules := []*s3.FilterRule{rule}
	keyFilter := &s3.KeyFilter{
		FilterRules: filterRules,
	}

	notificationConfigfilter := &s3.NotificationConfigurationFilter{
		Key: keyFilter,
	}

	event := "s3:ObjectCreated:*"
	events := []*string{&event}

	lambdaFunctionConfig := &s3.LambdaFunctionConfiguration{
		Events:            events,
		Filter:            notificationConfigfilter,
		LambdaFunctionArn: &lm.FunctionArn,
	}

	lambdaFunctionConfigs := []*s3.LambdaFunctionConfiguration{lambdaFunctionConfig}
	input := &s3.PutBucketNotificationConfigurationInput{
		Bucket: &bucket,
		NotificationConfiguration: &s3.NotificationConfiguration{
			LambdaFunctionConfigurations: lambdaFunctionConfigs,
		},
	}
	_, err := lm.S3Client.PutBucketNotificationConfiguration(input)
	return err
}

func ComputeBatchSize(allObjects []*s3.Object, lambdaMemory int) int {
	totalSizeOfDataset := 0.0

	for _, object := range allObjects {
		totalSizeOfDataset += float64(*object.Size)
	}

	avgObjectSize := totalSizeOfDataset / float64(len(allObjects))
	fmt.Printf("Dataset size (bytes) %f, nKeys: %d, avg size (bytes): %f\n", totalSizeOfDataset, len(allObjects), avgObjectSize)
	maxMemoryForDataset := 0.6 * float64(lambdaMemory*1000*1000)
	objectsPerBatch := int(maxMemoryForDataset / avgObjectSize) // Golang does not provide a round function in standard math
	return objectsPerBatch
}

func BatchCreator(allObjects []*s3.Object, objectsPerBatch int) [][]string {
	var batches [][]string
	var batch []string
	for _, object := range allObjects {
		batch = append(batch, *object.Key)
		if len(batch) == objectsPerBatch {
			batches = append(batches, batch)
			batch = []string{}
		}
	}

	// If there are objects in batch
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}
