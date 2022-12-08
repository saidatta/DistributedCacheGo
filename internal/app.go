package main

import (
	"DistributedCacheGo/internal/config"
	"DistributedCacheGo/internal/nodes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
)

var db = map[string][]byte{
	"1":   []byte("14514"),
	"11":  []byte("4514"),
	"114": []byte("514"),
}

//
//func main() {
//	var port int
//	flag.IntVar(&port, "port", 8001, "Node's port")
//	flag.Parse()
//	nodes := nodes.NewNode("pigeon", 377777, func(key string) ([]byte, error) {
//		r, b := db[key]
//		if b {
//			return r, nil
//		} else {
//			return nil, http.ErrServerClosed
//		}
//	})
//	nodes.StartNodeServer(":" + strconv.Itoa(port))
//}

//func main() {
//var port int
//flag.IntVar(&port, "port", 8001, "Node's port")
//flag.Parse()
//nodes := nodes.NewNode("pigeon", 377777, func(key string) ([]byte, error) {
//	r, b := db[key]
//	if b {
//		return r, nil
//	} else {
//		return nil, http.ErrServerClosed
//	}
//})
//
//nodes.StartNodeServer(":" + strconv.Itoa(port))
//}

func main() {
	file, err := os.Open("nodes.json")
	defer file.Close()
	if err != nil {
		log.Println("Config file : nodes.json not exists")
		return
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Println(err)
		return
	}
	var nodesInfo []config.NodeInfo
	err = json.Unmarshal(fileBytes, &nodesInfo)
	if err != nil {
		log.Println(err)
		return
	}
	var port int
	flag.IntVar(&port, "port", 7777, "Node Manager host")
	flag.Parse()
	manager := nodes.NewManger()
	for _, nodes := range nodesInfo {
		manager.AddNode(nodes.Name, nodes.HostAndPort)
	}

	manager.Register()
	manager.StartManageServer(":" + strconv.Itoa(port))

}
