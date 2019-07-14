package simplesqs

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
)

type MessageQueue struct {
	AccessKeyId  string
	SecretKeyId  string
	SessionToken string
	Region       string
	QueueName    string
	queueUrl     *string
	svc          *sqs.SQS
}

func (mq *MessageQueue) getSession() (*session.Session, error) {
	if mq.AccessKeyId == "" {
		config := aws.NewConfig().WithCredentialsChainVerboseErrors(true).WithRegion(mq.Region)
		return session.NewSession(config)
	}

	return session.NewSession(&aws.Config{
		Credentials:                   credentials.NewStaticCredentials(mq.AccessKeyId, mq.SecretKeyId, mq.SessionToken),
		Region:                        aws.String(mq.Region),
		CredentialsChainVerboseErrors: aws.Bool(true),
	})
}

func (mq *MessageQueue) createQueue() (*sqs.CreateQueueOutput, error) {
	return mq.svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(mq.QueueName),
	})
}

func (mq *MessageQueue) Init() error {
	if mq.Region == "" {
		return errors.New("No Region specified. Region is required.")
	}

	if mq.QueueName == "" {
		return errors.New("No QueueName specified. QueueName is required.")
	}

	sess, err := mq.getSession()
	if err != nil {
		log.Fatalf("Error getting session: %s", err)
		return err
	}
	mq.svc = sqs.New(sess)

	res, err := mq.createQueue()
	if err != nil {
		log.Fatalf("Error creating queue %s - %s", mq.QueueName, err)
		return err
	}
	mq.queueUrl = res.QueueUrl

	return nil
}

func convert(attributes map[string]string) map[string]*sqs.MessageAttributeValue {
	output := make(map[string]*sqs.MessageAttributeValue)

	for k, v := range attributes {
		output[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	return output
}

func (mq *MessageQueue) SendMessage(body string, attributes map[string]string) (*sqs.SendMessageOutput, error) {
	var res *sqs.SendMessageOutput
	var err error

	if len(attributes) > 0 {
		res, err = mq.svc.SendMessage(&sqs.SendMessageInput{
			DelaySeconds:      aws.Int64(0),
			MessageAttributes: convert(attributes),
			MessageBody:       aws.String(body),
			QueueUrl:          mq.queueUrl,
		})
	} else {
		res, err = mq.svc.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(body),
			QueueUrl:     mq.queueUrl,
		})
	}

	if err != nil {
		log.Fatalf("Error sending message to queue %s with body %s and attributes %#v\n. Error: %s\n", mq.QueueName, body, attributes, err)
	}

	return res, err
}

func convertMessages(messages []*sqs.Message) []string {
	output := make([]string, len(messages))
	for _, message := range messages {
		output = append(output, *message.Body)
	}

	return output
}

func (mq *MessageQueue) ReceiveMessage(maxMessages int64) ([]string, error) {
	res, err := mq.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            mq.queueUrl,
		MaxNumberOfMessages: aws.Int64(maxMessages),
		VisibilityTimeout:   aws.Int64(20),
		WaitTimeSeconds:     aws.Int64(0),
	})

	if err != nil {
		log.Fatalf("Error receiving message from queue %s with attributes %#v\n", mq.QueueName, "")
	}

	return convertMessages(res.Messages), err
}
