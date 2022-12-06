package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	sxmqtt "github.com/synerex/proto_mqtt"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	topic           = flag.String("topic", "", "Subscribe MQTT Topic")
	sxServerAddress string
	mu              sync.Mutex
	jsonClient      *sxutil.SXServiceClient
)

func supplyMQTTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	rcd := &sxmqtt.MQTTRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, rcd)
	if err == nil { // get MQTT Record
		//		ts0 := ptypes.TimestampString(rcd.Time)
		if *topic != "" {
			if strings.HasPrefix(rcd.Topic, *topic) {
				ld := fmt.Sprintf("%s:%s", rcd.Topic, string(rcd.Record))
				log.Print(ld)
				smo := sxutil.SupplyOpts{
					Name: "stdin",
					JSON: string(rcd.Record),
				}
				_, nerr := jsonClient.NotifySupply(&smo)
				if nerr != nil { // connection failuer with current client
					log.Printf("Connection failure", nerr)
				}
			}
		} else {
			ld := fmt.Sprintf("%s,%s", rcd.Topic, string(rcd.Record))
			log.Print(ld)
		}
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.SXClient != nil {
		client.SXClient = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.SXClient == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.SXClient = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server [%s]\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeMQTTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyMQTTCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func main() {
	log.Printf("MQTT_Subscriber(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.MQTT_GATEWAY_SVC, pbase.JSON_DATA_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "MQTT-Subscriber", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Clt:MQTT-to-JSON:%s}", *topic)
	mqttClient := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON)
	jsonClient = sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, argJSON)

	wg.Add(1)
	log.Print("Subscribe Topic ", *topic)
	go subscribeMQTTSupply(mqttClient)
	wg.Wait()

	sxutil.CallDeferFunctions() // cleanup!
}
