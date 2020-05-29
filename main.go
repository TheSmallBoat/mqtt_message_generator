package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
	"math/rand"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var failnums int = 0

func main() {
	var hostname, _ = os.Hostname()

	server := flag.String("server", "tcp://test.mosquitto.org:1883", "The broker URI. ex: tcp://10.10.1.1:1883")
	topic := flag.String("topic", "iotstream/data_generator", "Topic to publish the messages on (optional)")
	zone := flag.String("zone","zone_zero","The zone name for manage data (default zone_zero)")
	qos := flag.Int("qos", 0, "The QoS to send the messages at (default 0)")
	cleansess := flag.Bool("clean", false, "Set Clean Session (default false)")
	keepalive := flag.Uint("keepalive",60,"The seconds of the connect timeout (default 60)")
	maxsleep := flag.Int("maxsleep",8,"The maximum seconds to sleep between messages (default 8)")
	username := flag.String("username", "", "A username to authenticate to the MQTT server (optional)")
	password := flag.String("password", "", "Password to match username (optional)")
	messagenum := flag.Uint("messagenum", 1, "The number of messages to publish (default 1)")
	clientnum := flag.Uint("clientnum",1,"The number of clients (default 1)")
	flag.Parse()

	opts := MQTT.NewClientOptions()

	opts.AddBroker(*server)
	opts.SetUsername(*username)
	opts.SetPassword(*password)
	opts.SetCleanSession(*cleansess)
	opts.SetPingTimeout(1 * time.Second)

	opts.SetConnectTimeout(time.Duration(*keepalive) * time.Second)

	wg := sync.WaitGroup{}

	for i := 0; i < int(*clientnum); i++ {

		wg.Add(1)
		time.Sleep(5 * time.Millisecond)
		go createTask(i, hostname, *zone, *topic, *qos, *maxsleep, *messagenum, opts, &wg)
	}
	wg.Wait()
}

func createTask(taskid int, hostname string, zone string, topic string, qos int, maxsleep int,messagenum uint, opts *MQTT.ClientOptions, wg *sync.WaitGroup) {
	defer wg.Done()

	clientid := fmt.Sprintf("%s_gct%d_%d", hostname, taskid, time.Now().Unix())
	clienttopic := fmt.Sprintf("%s/%s/%s",topic,zone,clientid)
	opts.SetClientID(clientid)

	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		failnums++
		fmt.Printf("Task [%d] failed!, fail_nums : [%d] \n", taskid, failnums)
		panic(token.Error())
	}

	sleep := rand.Intn(maxsleep)+1
	fmt.Printf("[%s] ... Task [%d] will send (%d) message , sleep (%ds | %ds) \n",clienttopic,taskid,messagenum,sleep,maxsleep)

	for mid := 0; mid < int(messagenum); mid++ {
		time.Sleep(time.Duration(sleep) * time.Second)
		text := fmt.Sprintf("{\"c_id\":\"%s\",\"c_time\":%d,\"m_id\":%d,\"m_body\":\"task id : [%d]\"}", clientid, time.Now().Unix(), mid, taskid)
		token := client.Publish(clienttopic, byte(qos), false, text)
		token.Wait()
	}

	client.Disconnect(250)
	fmt.Printf("[%s] ... Task [%d] ok! ... [%d] failed. \n",clienttopic,taskid,failnums)
}
