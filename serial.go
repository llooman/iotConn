package main

/*
	ports2.put(1, "/dev/ttyUSB0"   );
	ports2.put(2, "/dev/ttyUSB1"   );
	ports2.put(3, "/dev/ttyUSB2"   );
	ports2.put(4, "/dev/ttyUSB3"   );
	ports2.put(5, "/dev/ttyUSB4"   );
	ports2.put(6, "/dev/ttyUSB5"   );
*/

import (
	"fmt"
	"iot"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	serial "github.com/tarm/serial"
)

var serialStatus [2]string

type iotSerial struct {
	name         string
	serialPort   *serial.Port
	subscription string
	status       string
}

var iotSerials [2]iotSerial

func startSerial(serialPorts []string, index int) {

	serialPort, portName, err := scanPorts(serialPorts)
	if err != nil {
		iot.Err.Printf("Serial scanPorts:%s", err.Error())
		return
	}

	go serialHandler(serialPort, portName, index)
}

func scanPorts(serialPorts []string) (*serial.Port, string, error) {

	var serialPort *serial.Port
	var err error

	for _, portName := range serialPorts {

		comPort := &serial.Config{Name: portName, Baud: 115200}
		serialPort, err = serial.OpenPort(comPort)

		if err == nil {
			return serialPort, portName, err
		}

		// fmt.Printf("Serial skip %s err:%s\n", portName, err.Error())
	}

	return serialPort, "noPortAvailable", err
}

func serialHandler(serialPort *serial.Port, portName string, index int) {

	iot.Info.Printf("Serial start %s", portName)

	defer serialPort.Close()

	var connID int
	var payload string
	var iotPayload iot.IotPayload
	// var svcLoop = "startServiceLoop"
	serialStatus[index] = "running"

	// mqClientID := fmt.Sprintf("%s%s", iotConfig.ServerId, portName)

	// if iot.LogCode("s") {
	// 	iot.Info.Printf("isvc.new clnt: %s\n", mqClientID)
	// }

	// mqClient := iot.NewMQClient(iotConfig.MqClient, mqClientID)
	subscription := ""

	//mqSubscribe(mqClient, topic, conn, subscription)
	// var n int
	var open bool
	var i int
	var j int
	var readBuf = make([]byte, 128)
	var writeBuf = make([]byte, 128)

	for serialStatus[index] != "stop" {

		n, err := serialPort.Read(readBuf)

		if err != nil {
			iot.Err.Printf("Serial %s:%s", portName, err.Error())
			serialStatus[index] = "stop"
			// subscribeDownQueueForSerial("", serialPort, subscription)
			if token := mqttClient.Unsubscribe(subscription); token.Wait() && token.Error() != nil {
				iot.Err.Printf("Failed mqtt.Unsubscribe %s:%s", subscription, token.Error())
			}
			continue
		}

		i = 0
		for i < n {

			if readBuf[i] == '{' {
				open = true

			} else if readBuf[i] == '}' {

				payload = string(writeBuf[:j])
				iotPayload = iot.ToPayload(payload)

				if iotPayload.ConnId > 0 &&
					connID != iotPayload.ConnId {

					topic := fmt.Sprintf("%s%d", iotConfig.MqttDown, iotPayload.ConnId)
					// err := subscribeDownQueueForSerial(mqClient, topic, serialPort, subscription)
					err := subscribeDownQueueForSerial(topic, serialPort, subscription)
					if err != nil {

						iot.Err.Println(err.Error())
						subscription = ""
						connID = 0
					} else {

						subscription = topic
						connID = iotPayload.ConnId
					}
				}

				if iot.LogPayload(&iotPayload) {

					iot.Info.Printf("{%s}\n", payload)
				}

				forwardUp(payload, &iotPayload)

				j = 0
				open = false
				time.Sleep(2 * time.Millisecond)

			} else if open {

				if j > 32 {
					iot.Err.Printf("writeBuf=%s\n", string(writeBuf[:j]))
					j = 0
				} else {
					writeBuf[j] = readBuf[i]
					j++
				}
			}
			i++
		}
	}
}

// func subscribeDownQueueForSerial(client mqtt.Client, topic string, serialPort *serial.Port, prevTopic string) error {
func subscribeDownQueueForSerial(topic string, serialPort *serial.Port, prevTopic string) error {

	if topic == prevTopic {
		return nil
	}

	if len(prevTopic) > 0 {
		if token := mqttClient.Unsubscribe(prevTopic); token.Wait() && token.Error() != nil {
			iot.Err.Println(token.Error())
			return token.Error()
			// os.Exit(1)
		}
	}

	if len(topic) == 0 {
		iot.Info.Printf("Stop subscription")
		return nil
	}

	token := mqttClient.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {

		payload := string(msg.Payload())
		iotPayload := iot.ToPayload(payload)

		if iot.LogPayload(&iotPayload) {
			iot.Info.Printf("mq.received [%s] %s\n", topic, payload)
		}

		_, err := serialPort.Write([]byte(payload + "\n"))
		if err != nil {
			iot.Err.Printf("Serial Write:%s", err.Error())
		}

	})

	if token.Error() != nil {
		iot.Err.Printf("Serial.Subscribe.err %s:topic[%s]\n", token.Error(), topic)
	} else {
		iot.Info.Printf("Serial.subscribe [%s]", topic)
	}

	return token.Error()
}
