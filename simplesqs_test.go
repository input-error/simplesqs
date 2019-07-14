package simplesqs

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"testing"
)

func TestInit_RegionRequired(t *testing.T) {
	t.Parallel()

	tq := &MessageQueue{
		QueueName: "testQueue",
	}

	err := tq.Init()

	if err == nil {
		t.Error("MessageQueue.Init should return error when Region is not specified.")
	}
}

func TestInit_QueueRequired(t *testing.T) {
	t.Parallel()

	tq := &MessageQueue{
		Region: "us-east-1",
	}

	err := tq.Init()

	if err == nil {
		t.Error("MessageQueue.Init should return error when QueueName is not specified.")
	}
}

func TestInit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestInit in short mode")
	}

	tq := &MessageQueue{
		QueueName: "testQueue",
		Region:    "us-east-1",
	}

	err := tq.Init()
	if err != nil {
		t.Errorf("Init() failed received error: %s", err)
	}
}

func TestConvert(t *testing.T) {
	t.Parallel()

	var convertedAttributes map[string]*sqs.MessageAttributeValue
	attributesToConvert := map[string]string{
		"bucket": "testBucket",
	}

	convertedAttributes = convert(attributesToConvert)

	if *convertedAttributes["bucket"].DataType != "String" {
		t.Error("DataType is not String")
	}

	if *convertedAttributes["bucket"].StringValue != "testBucket" {
		t.Error("StringValue is not testBucket")
	}
}

func TestConvert_nil(t *testing.T) {
	t.Parallel()

	var convertedAttributes map[string]*sqs.MessageAttributeValue
	convertedAttributes = convert(nil)

	if len(convertedAttributes) != 0 {
		t.Logf("convertedAttributes has value of %#v\n", convertedAttributes)
		t.Error("len(convertedAttributes) was greater than 0")
	}
}

func TestSendMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestSendMessage in short mode")
	}

	tq := TestData{}
	tq.Init()

	tq.MQ.SendMessage("{ 'bucket': 'testBucket', 'filename': 'test.txt' }", map[string]string{"bucket": "testBucket"})
}

func TestSendMessage_case1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestSendMessage in short mode")
	}

	tq := TestData{}
	tq.Init()

	_, err := tq.MQ.SendMessage("This is a test!", map[string]string{})

	if err != nil {
		t.Errorf("SendMessage returned an error when attempting to send.")
	}
}

func TestReceiveMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestReceiveMessage in short mode")
	}

	tq := TestData{}
	tq.Init()

	tq.MQ.SendMessage("{ 'bucket': 'testBucket', 'filename': 'test.txt' }", map[string]string{"bucket": "testBucket"})
	messages, err := tq.MQ.ReceiveMessage(1)

	if err != nil {
		t.Errorf("error receiving messages. Error %s", err)
	}

	if len(messages) < 1 || len(messages) > 1 {
		t.Errorf("Returned messages count not equal to 1. len(messages) returns: %d", len(messages))
	}
}
