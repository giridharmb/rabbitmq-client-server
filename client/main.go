package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

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
		log.Fatalf("(ERROR) : %s: %s", msg, err)
	}

}

type MQSettings struct {
	IPAddr string
	USER   string
	PWD    string
}

var settings = MQSettings{
	IPAddr: "",
	USER:   "",
	PWD:    "",
}

func initialize() (bool, error) {
	rmqHOST := os.Getenv("RMQHOST")
	if rmqHOST == "" {
		return false, errors.New("RMQHOST environment variable is not set")
	}
	rmqUSER := os.Getenv("RMQUSER")
	if rmqUSER == "" {
		return false, errors.New("RMQUSER environment variable is not set")
	}
	rmqPWD := os.Getenv("RMQPWD")
	if rmqPWD == "" {
		return false, errors.New("RMQPWD environment variable is not set")
	}
	settings.IPAddr = rmqHOST
	settings.USER = rmqUSER
	settings.PWD = rmqPWD
	return true, nil
}

func pushMessageOnChannel(queueName string, data interface{}, amqpChannel *amqp.Channel) error {

	queue, err := amqpChannel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return errors.New(fmt.Sprintf("could not declare queue (%v) : %v", queueName, err.Error()))
	}

	body, err := json.Marshal(data)
	if err != nil {
		return errors.New(fmt.Sprintf("error in encoding JSON : %v", err.Error()))
	}

	err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})
	if err != nil {
		return errors.New(fmt.Sprintf("error in pushing message : %v", err.Error()))
	}
	return nil
}

func main() {
	fmt.Printf("\nRabbitMQ Client\n")
	_, err := initialize()
	if err != nil {
		fmt.Printf("\n(ERROR) : %v", err.Error())
		return
	}

	MQConfig.AMQPConnectionURL = fmt.Sprintf("amqp://%v:%v@%v:5672/", settings.USER, settings.PWD, settings.IPAddr)

	conn, err := amqp.Dial(MQConfig.AMQPConnectionURL)
	handleError(err, "can't connect to AMQP")
	defer func() {
		_ = conn.Close()
	}()

	amqpChannel, err := conn.Channel()
	handleError(err, "can't create a amqpChannel")

	defer func() {
		_ = amqpChannel.Close()
	}()

	//////////////////////////////////////////////////////////
	// push an array of map[string]string -> (on queue1)
	//////////////////////////////////////////////////////////

	myRandomDataStr := fmt.Sprintf(`[{"random":30,"firstname":"Jean","lastname":"Orpah","city":"Tallinn","country":"Bermuda"},{"random":32,"firstname":"Oralee","lastname":"Dannye","city":"Vancouver","country":"Tanzania,UnitedRepublicof"},{"random":47,"firstname":"Etta","lastname":"Hartnett","city":"Oranjestad","country":"Bahrain"},{"random":46,"firstname":"Melanie","lastname":"Jotham","city":"Barranquilla","country":"Gibraltar"},{"random":48,"firstname":"Nataline","lastname":"Corilla","city":"Malmö","country":"Gibraltar"},{"random":28,"firstname":"Helena","lastname":"Calhoun","city":"Chihuahua","country":"Reunion"},{"random":20,"firstname":"Misha","lastname":"Olnee","city":"Arequipa","country":"Kazakhstan"},{"random":88,"firstname":"Alie","lastname":"Iiette","city":"PagoPago","country":"Djibouti"},{"random":64,"firstname":"Doro","lastname":"Margarete","city":"QuezonCity","country":"Honduras"},{"random":97,"firstname":"Dyann","lastname":"Buckler","city":"Willemstad","country":"TrinidadandTobago"},{"random":13,"firstname":"Kathy","lastname":"Old","city":"SanFernando","country":"Martinique"},{"random":50,"firstname":"Rosabelle","lastname":"Elvyn","city":"TheHague","country":"Colombia"},{"random":75,"firstname":"Carly","lastname":"Abbot","city":"Marrakech","country":"SvalbardandJanMayen"},{"random":96,"firstname":"Winny","lastname":"Sprage","city":"Kathmandu","country":"Sudan"},{"random":37,"firstname":"Lorenza","lastname":"Gordon","city":"Belgrade","country":"TurksandCaicosIslands"},{"random":79,"firstname":"Carilyn","lastname":"Kirstin","city":"Ouagadougou","country":"Iceland"},{"random":58,"firstname":"Joane","lastname":"Ader","city":"Bratsk","country":"Nepal"},{"random":91,"firstname":"Gwenneth","lastname":"Monk","city":"Leipzig","country":"TrinidadandTobago"},{"random":65,"firstname":"Veda","lastname":"Rocray","city":"Cannes","country":"Kenya"},{"random":85,"firstname":"Philis","lastname":"Shaver","city":"Xi'an","country":"Indonesia"}]`)

	myRandomData := make([]interface{}, 0)

	_ = json.Unmarshal([]byte(myRandomDataStr), &myRandomData)

	stopChanQueue1 := make(chan struct{})
	stopChanQueue2 := make(chan struct{})

	go func() {
		for _, data := range myRandomData {
			myGenericData := MessageData{Data: data.(map[string]interface{})}
			err := pushMessageOnChannel("queue1", myGenericData, amqpChannel)
			if err != nil {
				fmt.Printf("(ERROR) : %v", err.Error())
				close(stopChanQueue1)
				break
			}
			log.Printf("queue1 (push) : Data: myGenericData : %v", myGenericData)
			time.Sleep(500 * time.Millisecond)
		}
		close(stopChanQueue1)
	}()

	//////////////////////////////////////////////////////////
	// Push Numbers as Strings -> (on queue2)
	//////////////////////////////////////////////////////////

	go func() {
		for i := 1; i < 40; i++ {
			rand.Seed(time.Now().UnixNano())
			randomNumber := rand.Intn(999)
			randomNumberString := strconv.Itoa(randomNumber)
			myMessageData := MessageData{Data: randomNumberString}
			err := pushMessageOnChannel("queue2", myMessageData, amqpChannel)
			if err != nil {
				fmt.Printf("(ERROR) : %v", err.Error())
				close(stopChanQueue2)
				break
			}
			log.Printf("queue2 (push) : Data: %v", myMessageData)
			time.Sleep(200 * time.Millisecond)
		}
		close(stopChanQueue2)
	}()

	fmt.Printf("\nwaiting for data to be sent from both the queues...\n")
	<-stopChanQueue1
	<-stopChanQueue2
	fmt.Printf("\ndone sending data from both the queues.")
}
