package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
)

func supplyMQTTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	rcd := &sxmqtt.MQTTRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, rcd)
	if err == nil { // get MQTT Record
		//		ts0 := ptypes.TimestampString(rcd.Time)
		ld := fmt.Sprintf("%s,%s", rcd.Topic, string(rcd.Record))
		log.Print(ld)
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
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

	channelTypes := []uint32{pbase.MQTT_GATEWAY_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "MQTT-Subscriber", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Clt:MQTT-Sub:%s}", *topic)
	sclient := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON)

	wg.Add(1)
	log.Print("Subscribe Topic ", *topic)
	go subscribeMQTTSupply(sclient)
	wg.Wait()

	sxutil.CallDeferFunctions() // cleanup!
}
