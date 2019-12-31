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
var debugSerial bool

type IotSerial struct {
	index        int
	name         string
	serialPort   *serial.Port
	subscription string
	status       string
	retry        int
}

// IotSerials ...
var IotSerials [2]IotSerial

func initSerials() {

	// debugSerial = true
	IotSerials[0].status = "idle"
	IotSerials[0].index = 1

	IotSerials[1].status = "idle"
	IotSerials[1].index = 2
}

func checkSerials() {

	if IotSerials[0].status == "stop" ||
		IotSerials[0].status == "idle" {

		startSerial(&IotSerials[0], iotConfig.SerialPorts)

	}

	if IotSerials[1].status == "stop" ||
		IotSerials[1].status == "idle" {

		startSerial(&IotSerials[1], iotConfig.SerialPorts)
	}
}

func startSerial(iotSerial *IotSerial, serialPorts []string) {

	if debugSerial {
		iot.Info.Printf("startSerial %d", iotSerial.index)
	}

	scanPorts(iotSerial, serialPorts)

	if iotSerial.status == "connected" {
		go serialHandler(iotSerial)
	}
}

func portInUse(portName string) bool {

	for _, iotSerial := range IotSerials {

		if portName == iotSerial.name &&
			iotSerial.status == "connected" {

			return true
		}
	}
	return false
}

func scanPorts(iotSerial *IotSerial, serialPorts []string) {

	var err error

	for _, portName := range serialPorts {

		if portInUse(portName) {

			iot.Info.Printf("scanPorts.inUse %s", portName)

		} else {

			if debugSerial {
				iot.Info.Printf("scanPorts.OpenPort %s", portName)
			}
			comPort := &serial.Config{Name: portName, Baud: 115200}
			iotSerial.serialPort, err = serial.OpenPort(comPort)

			if err == nil {
				iotSerial.name = portName
				iotSerial.status = "connected"
				iot.Info.Printf("scanPorts.connected %s", portName)
				return
			}
		}
	}

	iotSerial.status = "idle"
}

func serialHandler(iotSerial *IotSerial) {

	iot.Info.Printf("SerialHandler %d %s", iotSerial.index, iotSerial.name)

	defer iotSerial.serialPort.Close()

	subscription := ""

	var connID int
	var payload string
	var iotPayload iot.IotPayload
	var open bool
	var i int
	var j int
	var readBuf = make([]byte, 128)
	var writeBuf = make([]byte, 128)

	for iotSerial.status != "stop" {

		n, err := iotSerial.serialPort.Read(readBuf)

		if err != nil {

			iot.Err.Printf("Serial %s:%s", iotSerial.name, err.Error())
			iotSerial.status = "stop"
			if token := mqttClient.Unsubscribe(subscription); token.Wait() && token.Error() != nil {
				iot.Err.Printf("mqtt.Unsubscribe %s:%s", subscription, token.Error())
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
					err := subscribeDownQueueForSerial(topic, iotSerial.serialPort, subscription)
					if err != nil {

						iot.Err.Println(err.Error())
						subscription = ""
						connID = 0
					} else {

						subscription = topic
						iot.Info.Printf("SerialSubscribe %d %s", iotSerial.index, subscription)
						connID = iotPayload.ConnId
					}
				}

				if iot.LogPayload(&iotPayload) || debugSerial {

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

func subscribeDownQueueForSerial(topic string, serialPort *serial.Port, prevTopic string) error {

	if topic == prevTopic {
		return nil
	}

	if len(prevTopic) > 0 {
		if token := mqttClient.Unsubscribe(prevTopic); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}

	if len(topic) == 0 {
		iot.Info.Printf("Stop subscription")
		return nil
	}

	token := mqttClient.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {

		payload := string(msg.Payload())
		iotPayload := iot.ToPayload(payload)

		if iot.LogPayload(&iotPayload) || debugSerial {
			iot.Info.Printf("mq.received [%s] %s\n", topic, payload)
		}

		_, err := serialPort.Write([]byte(payload + "\n"))
		if err != nil {
			iot.Err.Printf("Serial Write:%s", err.Error())
		}

	})

	return token.Error()
}
