/*   xxxx  xxxx
Connector 2.0

*/

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"

	"iot"
	// "iot/svc/verw"

	// "path"

	"runtime"
	"strings"
	"time"

	"github.com/tkanos/gonfig"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	_ "github.com/go-sql-driver/mysql"
)

type IotConfig struct {
	Port   string
	MqttUp string

	MqClient string
	MqID     string
	Database string

	Debug   bool
	Logfile string

	ServerId string
	Local    bool
}

var raspbian3 iot.IotConn
var iotConfig IotConfig
var mqttClient mqtt.Client

var databaseNew *sql.DB

var refresherRunning bool
var nextRefresh int64
var prevRefresh int64

var meNode *iot.IotNode
var mePing *iot.IotProp
var logLevel *iot.IotProp

func init() {
	iot.LogDebug = false
}

func main() {

	fmt.Printf("... ")

	//defer iot.Info.Print("****  stop  ****")

	err := gonfig.GetConf(iot.Config(), &iotConfig)
	checkError(err)

	fmt.Printf(" %s ...\n", iotConfig.Logfile)

	f, err := os.OpenFile(iotConfig.Logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644) //O_RDWR
	if err != nil {
		// fmt.Printf(" error reading config %s ...\n", iotConfig.Logfile)
		iot.Err.Printf("%s: %v", iotConfig.Logfile, err.Error())
	}

	defer f.Close()

	if iotConfig.Local {
		iot.InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)

	} else {
		iot.InitLogging(ioutil.Discard, f, f, f)
	}

	/*
	  ioutil.Discard  os.Stdout  os.Stderr
	*/

	iot.Info.Print("****  start  ****")

	iot.DatabaseNew, err = sql.Open("mysql", iotConfig.Database)
	checkError(err)
	defer databaseNew.Close()
	iot.Info.Print("DB Connected")

	meNode = iot.GetOrDefaultNode(1, "conn")
	logLevel = iot.GetNodeProp(meNode, 10)
	mePing = iot.GetNodeProp(meNode, 5)

	mqttClient = iot.NewMQClient(iotConfig.MqClient, iotConfig.MqID)

	service := "0.0.0.0:" + iotConfig.Port
	if runtime.GOOS == "windows" {
		service = "localhost:" + iotConfig.Port
	}

	err = startConnector(service)
	checkError(err)

	handleIotOut(mqttClient, "iotOut/1")
	iot.Info.Print("subscribe iotOut/1")

	var nextWatchDog int64

	// pingLoad := "{U,1,5,1}"
	// iotPing := iot.ToPayload(pingLoad)

	// logLevelLoad := "{R,1,10,1}"
	// iotLogLevel := iot.ToPayload(pingLoad)

	// forwardUp(logLevelLoad, &iotLogLevel)

	getDefault(logLevel)

	if iotConfig.Local {
		// fmt.Println("NO SCHEDULER, NO REFRESHER, NO VERWARMING !!!")
	}

	for true {

		if nextWatchDog == 0 ||
			(nextWatchDog < time.Now().Unix() &&
				time.Now().Second()%10 == 0) {

			nextWatchDog = time.Now().Unix() + 7

			err = iot.DatabaseNew.Ping()
			if err != nil {
				iot.Err.Printf("database.Ping() err: " + err.Error())
				fmt.Fprintf(os.Stderr, "iot: Fatal error: %s", err.Error())
				os.Exit(1)
			}

			up(mePing)
			// fmt.Printf("%v  ", iotPing)
			// fmt.Printf(" %s", pingLoad)
			// forwardUp(pingLoad, &iotPing)
		}

		time.Sleep(995 * time.Millisecond)
	}

	iot.Err.Printf("conn stop\n")
}

func handleIotOut(client mqtt.Client, topic string) {

	token := client.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {

		payload := string(msg.Payload())

		// if iot.LogDebug {
		// 	iot.Info.Printf("mq.received [%s] %s\n", topic, payload)
		// }

		iotPayload := iot.ToPayload(payload)

		if iot.LogPayload(&iotPayload) {
			iot.Info.Printf("handleIotOut: %s, %v", payload, iotPayload)
		}

		switch iotPayload.Cmd {

		case "set", "s", "S":
			if iotPayload.NodeId == 1 &&
				iotPayload.PropId == 10 {

				logLevel.ValString = iotPayload.Val
				iot.OnChangeLogOptions(logLevel.ValString)

				up(logLevel)

			}

		case "R", "r":

		default:
		}

	})

	if token.Error() != nil {
		iot.Err.Printf("handleIotOut %s:topic[%s]", token.Error(), topic)
	}
}

func getDefault(prop *iot.IotProp) {

	topic := fmt.Sprintf("%s%d", iotConfig.MqttUp, 1)
	payload := fmt.Sprintf("{D,%d,%d,%d}", prop.Node.NodeId, prop.PropId, prop.Node.ConnId)

	if iot.LogProp(prop) {
		iot.Info.Printf("default %s [%s]", payload, topic)
	}

	token := mqttClient.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {

		iot.Err.Printf("error %s [%s]\n", payload, iotConfig.MqttUp)
		iot.Err.Printf("Fail to publish, %v", token.Error())
	}
}

func up(prop *iot.IotProp) {

	var payload string
	topic := fmt.Sprintf("%s%d", iotConfig.MqttUp, 1)

	if prop.IsStr {
		payload = fmt.Sprintf("{U,%d,%d,%d,%s}", prop.Node.NodeId, prop.PropId, prop.Node.ConnId, prop.ValString)
	} else {
		payload = fmt.Sprintf("{U,%d,%d,%d,%d}", prop.Node.NodeId, prop.PropId, prop.Node.ConnId, prop.Val)
	}

	if iot.LogProp(prop) {
		iot.Info.Printf("up %s [%s]", payload, topic)
	}

	token := mqttClient.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {

		iot.Err.Printf("error %s [%s]\n", payload, iotConfig.MqttUp)
		iot.Err.Printf("Fail to publish, %v", token.Error())
	}
}

func forwardUp(payload string, iotPayload *iot.IotPayload) {

	topic := iotConfig.MqttUp + strconv.Itoa(iotPayload.NodeId)
	// if iot.LogDebug{

	if iot.LogPayload(iotPayload) {
		iot.Info.Printf("forwardUp %s [%s]\n", payload, topic)
	}

	token := mqttClient.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {
		iot.Err.Printf("error %s [%s]\n", payload, iotConfig.MqttUp)
		iot.Err.Printf("Fail to publish, %v", token.Error())

	}
}

//----------------------------------------------------------------------------------------
// use todo list in mem
// refresh one at the time

//----------------------------------------------------------------------------------------

func startConnector(service string) error {
	var err error
	var tcpAddr *net.TCPAddr

	tcpAddr, err = net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return err
	}

	tcpListener, errr := net.ListenTCP("tcp4", tcpAddr)
	if errr != nil {
		return errr
	}

	go connListener(tcpListener)

	iot.Info.Printf("%s started\n", service)

	return err
}

func connListener(listener *net.TCPListener) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// multi threading:
		go connHandler(conn)
	}
}

func mqSubscribe(client mqtt.Client, topic string, conn net.Conn, prevTopic string) {

	if topic == prevTopic {
		return
	}

	if len(prevTopic) > 0 {
		if token := client.Unsubscribe(prevTopic); token.Wait() && token.Error() != nil {
			iot.Err.Println(token.Error())
			os.Exit(1)
		}
	}

	token := client.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {
		mqPayload := string(msg.Payload())

		// if iot.LogDebug {
		// 	iot.Info.Printf("mq.received [%s] %s\n", topic, mqPayload)
		// }

		conn.Write([]byte(mqPayload))
		conn.Write([]byte("\n"))
	})

	if token.Error() != nil {
		iot.Err.Printf("Subscribe.err %s:topic[%s]\n", token.Error(), topic)
	}

	// fmt.Printf("Subscribe [%s] \n", topic)
}

func connHandler(conn net.Conn) {

	//TODO prevent long request attack

	buff := make([]byte, 1024)
	var iotPayload iot.IotPayload
	var svcLoop = "startServiceLoop"

	clientAddr := conn.RemoteAddr().String()
	mqClientID := fmt.Sprintf("%sicon_%s", iotConfig.ServerId, clientAddr[strings.Index(clientAddr, ":")+1:len(clientAddr)])

	if iot.LogCode("s") {
		iot.Info.Printf("isvc.new clnt: %s\n", mqClientID)
	}

	mqClient := iot.NewMQClient(iotConfig.MqClient, mqClientID)
	subscription := ""

	defer conn.Close() // close connection before exit

	for svcLoop != "stop" {

		conn.SetReadDeadline(time.Now().Add(49 * time.Second)) // 49

		n, err := conn.Read(buff)

		if err != nil {

			if err.Error() == "EOF" ||
				strings.HasSuffix(err.Error(), "closed by the remote host.") ||
				strings.HasSuffix(err.Error(), "connection reset by peer") {

				iot.Trace.Printf("icon read:%s", err.Error())
				return
			}

			iot.Err.Printf("icon: %s", err.Error())
			return
		}

		payloadBuf := strings.TrimSpace(string(buff[0:n]))

		payloadArr := strings.Split(payloadBuf, "\n")

		for _, payload := range payloadArr {

			iotPayload = iot.ToPayload(payload)

			iot.Trace.Printf("conn[%s].payload<%s\n", mqClientID, payload)

			switch iotPayload.Cmd {

			case "close", "stop":
				iot.Trace.Printf("svc.payload<%s\n", payload)
				svcLoop = "stop"
				conn.Write([]byte("{\"retcode\":0}\n"))

			case "subscribe":

				topic := iotPayload.Val
				if iot.LogCode("s") {
					iot.Info.Printf("%s>%s\n", iotPayload.Cmd, topic)
				}
				mqSubscribe(mqClient, topic, conn, subscription)
				conn.Write([]byte("\n"))
				subscription = topic
			default:
				if len(payload) > 2 {
					commandAndRespond(payload, &iotPayload, conn)
				}
			}

		}
	}
}

func commandAndRespond(payload string, iotPayload *iot.IotPayload, conn net.Conn) {

	var respBuff bytes.Buffer

	switch iotPayload.Cmd {

	default:
		respBuff.Write([]byte(command(payload, iotPayload)))
	}

	//fmt.Printf("commandAndRespond: %s\n", iotPayload.Cmd)

	if respBuff.Len() < 1 {
		// conn.Write([]byte("{\"retcode\":0}"))

	} else if respBuff.String()[0:1] == "{" {
		conn.Write(respBuff.Bytes())
		conn.Write([]byte("\n"))

	} else {
		resp := fmt.Sprintf("{\"retcode\":99,\"message\":\"%s\"}", respBuff.String())
		conn.Write([]byte(resp))
		conn.Write([]byte("\n"))
	}

}

func command(payload string, iotPayload *iot.IotPayload) string {

	//fmt.Printf("command: %v\n", iotPayload)

	switch iotPayload.Cmd {

	case "ping":
		return ""

	}

	switch iotPayload.NodeId {

	case 1:
		return localCommand(iotPayload)

	default:
		forwardUp(payload, iotPayload)
	}

	return ""
}

func localCommand(iotPayload *iot.IotPayload) string {

	// fmt.Printf("localCommand: %v\n", iotPayload)
	// forward to the old iotSvc
	iot.Info.Printf("Conn.localCommand:%s val:%s", iotPayload.Cmd, iotPayload.Val)

	switch iotPayload.Cmd {

	case "R":
		return ""

	case "set", "S":
		// fmt.Printf("command %v\n", iotPayload)

		if iotPayload.PropId == 10 {

			iot.OnChangeLogOptions(iotPayload.Val)
			return setStrProp(logLevel, iotPayload.Val, iotPayload.Timestamp)

		} else {
			return fmt.Sprintf(`{"retcode":99,"message":"set prop %d not found"}`, iotPayload.PropId)
		}

	default:
		return fmt.Sprintf(`{"retcode":99,"message":"cmd %s not found"}`, iotPayload.Cmd)
	}

	return ""
}

func setStrProp(prop *iot.IotProp, newVal string, timestamp int64) string {

	if iot.LogProp(prop) {
		iot.Info.Printf("setStrProp isStr=%t n%dp%d:%d > %s ts:%d \n", prop.IsStr, prop.Node.NodeId, prop.PropId, prop.Val, newVal, timestamp)
	}

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	} else if timestamp > 3000000000 {
		timestamp = timestamp / 1000
	}

	if prop.IsStr {
		prop.ValString = newVal
		prop.ValStamp = timestamp

		iot.OnNodePropSave(prop)

		return ""

	} else {

		i64, err := strconv.ParseInt(newVal, 10, 32)
		if err != nil {
			iot.Err.Printf("SetStrProp err parsing newVal to Int for prop:%v", prop)
			return fmt.Sprintf("SetStrProp err parsing newVal to Int for prop:%v", prop)
		}

		prop.Val = i64
		prop.ValStamp = timestamp

		iot.OnNodePropSave(prop)

		return ""
	}
}

// func setProp(prop *iot.IotProp, newVal string, timestamp int64) string {

// 	i64, err := strconv.ParseInt(newVal, 10, 32)

// 	if err != nil {
// 		return fmt.Sprintf("SetStrProp err parsing newVal to Int for prop:%v", prop)
// 	}

// 	if timestamp == 0 {
// 		timestamp = time.Now().Unix()
// 	}

// 	prop.Val = i64
// 	prop.ValStamp = timestamp

// 	return ""

// }

func checkError(err error) {
	if err != nil {
		iot.Err.Println(err)
	}
}
func checkError2(err error) {
	if err != nil {
		iot.Err.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
