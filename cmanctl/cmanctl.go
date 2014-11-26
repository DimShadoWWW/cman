package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"log"
)

type Add struct {
	Source     string `json:"source"`
	Target     string `json:"target"`
	Continuous bool   `json:"continuous"`
	Create     bool   `json:"create_target"`
}

func (a *Add) String() string {
	cmd, err := json.Marshal(a)
	if err != nil {
		return ""
	}
	return "'" + string(cmd) + "'"
}

func (a *Add) Id() string {
	return "{{.HOSTNAME}}_{{.PORT}}_{{.DATABASE}}"
}

type ActionCfg struct {
	Key       string   `json:"Key"`       // Etcd key to look for couchdb nodes
	PutCmd    []string `json:"PutCmd"`    // Command to run to add
	PostCmd   []string `json:"PostCmd"`   // Command to run to add
	DeleteCmd []string `json:"DeleteCmd"` // Command to run to delete
	Add       Add      `json:"Add"`       // Command's params to add replication
}

func addAction(name, config string) error {
	_, err := client.Set(etcdKeyPrefix+"/actions/"+name+"/config", config, 0)
	if err != nil {
		return err
	}
	return nil
}

var (
	etcdUrl       string
	actionName    string
	etcdKeyPrefix string
	client        *etcd.Client
)

func main() {

	flag.StringVar(&etcdUrl, "etcd", "http://127.0.0.1:4001", "Etcd url")
	flag.StringVar(&etcdKeyPrefix, "etcd-key-prefix", "/couchdb-mng", "Keyspace for data in etcd")
	flag.StringVar(&actionName, "name", "action1", "Action name")
	flag.Parse()

	log.Println("Starting ...")
	client = etcd.NewClient([]string{etcdUrl})

	var action ActionCfg
	action.Key = "/skydns/local/home/production/couchdb"
	action.PutCmd = []string{"/usr/bin/curl", "-X", "PUT"}
	action.PostCmd = []string{"/usr/bin/curl", "--trace", "/home/core/curl_trace", "-X", "POST", "-H", "\"Content-Type: application/json\""}
	action.DeleteCmd = []string{"/usr/bin/curl", "-X", "DELETE"}
	action.Add.Source = "http://{{.HOSTNAME}}:{{.PORT}}/{{.DATABASE}}"
	action.Add.Target = "http://{{if .AUTH}}{{.AUTH}}{{end}}{{.SERVER_IP}}:{{.SERVER_PORT}}/{{.DATABASE}}"
	action.Add.Continuous = true
	action.Add.Create = true

	config, err := json.Marshal(action)

	if err != nil {
		fmt.Println(err)
	}
	// {
	//         "_id": "{{.HOSTNAME}}_{{.PORT}}_{{.DATABASE}}",
	//         "continuous": true,
	//         "source": "http://{{.HOSTNAME}}:{{.PORT}}/{{.DATABASE}}",
	//         "target": "{{.DATABASE}}",
	//         "create_target": true
	//     },
	//     "Del": {
	//         "_id": "{{.HOSTNAME}}_{{.PORT}}_{{.DATABASE}}"

	log.Println("Inserting ", string(config))
	err = addAction(actionName, string(config))
	if err != nil {
		fmt.Println(err)
	}
}
