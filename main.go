package main

import (
	"encoding/csv"
	"io"
	"log"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

const tableName = "CreditCardTransactions"
const maxBatchSize = 25

type S3CloudWatchEvent struct {
	Detail struct {
		Reason string `json:"reason"`
		Object struct {
			Key  string `json:"key"`
			Size int    `json:"size"`
		} `json:"object"`

		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
	} `json:"detail"`
}

func makeItem(id string, record []string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"Id": {
			S: aws.String(id),
		},
		"TransactionDate": {
			S: aws.String(record[0]),
		},
		"PostedDate": {
			S: aws.String(record[1]),
		},
		"CardLastFour": {
			S: aws.String(record[2]),
		},
		"Description": {
			S: aws.String(record[3]),
		},
		"Category": {
			S: aws.String(record[4]),
		},
		"Debit": {
			S: aws.String(record[5]),
		},
		"Credit": {
			S: aws.String(record[6]),
		},
	}
}

func storeItems(dynamoDBClient *dynamodb.DynamoDB, tableName string, itemList []*dynamodb.WriteRequest) {
	numberOfItems := len(itemList)

	if numberOfItems > 0 {
		log.Printf("Writing %d item(s) to table %s", numberOfItems, tableName)

		requestItems := map[string][]*dynamodb.WriteRequest{
			tableName: itemList,
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}

		_, err := dynamoDBClient.BatchWriteItem(input)
		if err != nil {
			log.Println(err)
		}
	}
}

func HandleRequest(event S3CloudWatchEvent) error {

	awsSession := session.Must(session.NewSession())
	dynamoDBClient := dynamodb.New(awsSession)
	s3Client := s3.New(awsSession)

	bucketName := event.Detail.Bucket.Name
	objectKey := event.Detail.Object.Key
	objectSize := event.Detail.Object.Size

	log.Printf("bucket name: %s, object key: %s, object size: %d\n", bucketName, objectKey, objectSize)

	result, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v - due to error: %v\n", bucketName, objectKey, err)
	}
	defer result.Body.Close()

	csvRecords := csv.NewReader(result.Body)
	counter := 0
	itemList := []*dynamodb.WriteRequest{}

	for {
		record, err := csvRecords.Read()
		if err == io.EOF {
			storeItems(dynamoDBClient, tableName, itemList)
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if counter == 0 { // Skip the header row
			counter++
			continue
		}

		item := makeItem(strconv.Itoa(counter), record)
		itemList = append(itemList, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: item}})

		if len(itemList) == maxBatchSize {
			storeItems(dynamoDBClient, tableName, itemList)
			itemList = []*dynamodb.WriteRequest{}
		}

		counter++
	}

	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
