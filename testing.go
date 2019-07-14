package simplesqs

type TestData struct {
	MQ        *MessageQueue
	QueueName string
	Region    string
}

func (td *TestData) Init() {
	td.Region = "us-east-1"
	td.QueueName = "testQueue"

	td.MQ = &MessageQueue{
		QueueName: td.QueueName,
		Region:    td.Region,
	}

	td.MQ.Init()
}
