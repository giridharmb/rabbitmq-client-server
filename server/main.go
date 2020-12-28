package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

/*
RabbitConfiguration ...
*/
type RabbitConfiguration struct {
	AMQPConnectionURL string
}

/*
MessageData ...
*/
type MessageData struct {
	Data interface{}
}

/*
MQConfig ...
*/
var MQConfig = RabbitConfiguration{
	AMQPConnectionURL: "",
}

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}

}

func getDeliveryChannel(queueName string, amqpChannel *amqp.Channel) <-chan amqp.Delivery {
	queue, err := amqpChannel.QueueDeclare(queueName, true, false, false, false, nil)
	handleError(err, fmt.Sprintf("Could not declare (%v) queue", queueName))

	err = amqpChannel.Qos(1, 0, false)
	handleError(err, "Could not configure QoS")

	messageChannel, err := amqpChannel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	handleError(err, "Could not register consumer")
	return messageChannel
}

func main() {

	rmqHOST := os.Getenv("RMQHOST")
	if rmqHOST == "" {
		fmt.Printf("\nRMQHOST environment variable is not set !")
		return
	}
	rmqUSER := os.Getenv("RMQUSER")
	if rmqUSER == "" {
		fmt.Printf("\nRMQUSER environment variable is not set !")
		return
	}
	rmqPWD := os.Getenv("RMQPWD")
	if rmqPWD == "" {
		fmt.Printf("\nRMQPWD environment variable is not set !")
		return
	}

	MQConfig.AMQPConnectionURL = fmt.Sprintf("amqp://%v:%v@%v:5672/", rmqUSER, rmqPWD, rmqHOST)

	conn, err := amqp.Dial(MQConfig.AMQPConnectionURL)
	handleError(err, "Can't connect to AMQP")
	defer func() {
		_ = conn.Close()
	}()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a amqpChannel")

	defer func() {
		_ = amqpChannel.Close()
	}()

	channel1 := getDeliveryChannel("queue1", amqpChannel)

	channel2 := getDeliveryChannel("queue2", amqpChannel)

	stopChan := make(chan bool)

	go func() {
		log.Printf("channel1 : Consumer ready, PID: %d", os.Getpid())
		for d := range channel1 {
			log.Printf("channel1 : Received a message: %s", d.Body)

			//addTask := &AddTask{}
			myMessageData := &MessageData{}

			//err := json.Unmarshal(d.Body, addTask)
			err := json.Unmarshal(d.Body, myMessageData)

			if err != nil {
				log.Printf("channel1 : Error decoding JSON: %s", err)
			}

			dataReceived := myMessageData.Data

			log.Printf("channel1 : Data Received : %v", dataReceived)

			if err := d.Ack(false); err != nil {
				log.Printf("channel1 : Error acknowledging message : %s", err)
			} else {
				log.Printf("channel1 : Acknowledged message")
			}
		}
	}()

	go func() {
		log.Printf("channel2 : Consumer ready, PID: %d", os.Getpid())
		for d := range channel2 {
			log.Printf("channel2 : Received a message: %s", d.Body)

			//addTask := &AddTask{}
			myMessageData := &MessageData{}

			//err := json.Unmarshal(d.Body, addTask)
			err := json.Unmarshal(d.Body, myMessageData)

			if err != nil {
				log.Printf("channel2 : Error decoding JSON: %s", err)
			}

			dataReceived := myMessageData.Data

			log.Printf("channel2 : Data Received : %v", dataReceived)

			if err := d.Ack(false); err != nil {
				log.Printf("channel2 : Error acknowledging message : %s", err)
			} else {
				log.Printf("channel2 : Acknowledged message")
			}
		}
	}()

	fmt.Printf("\nbefore <-stopChan \n")
	// Stop for program termination
	<-stopChan
	fmt.Printf("\nafter <-stopChan2 \n")
}
