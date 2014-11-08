package main

import (
	// "github.com/coreos/go-etcd/etcd"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRun(t *testing.T) {
	Convey("Creating a command for values", t, func() {
		cmd := "add {{.DATABASE}} -s {{.SERVER_IP}}:{{.SERVER_PORT}} -h {{.HOSTNAME}} -p {{.PORT}}"
		nodeAddress := "node1"
		nodePort := 2222
		serverAddress := "node2"
		serverPort := 1111
		dbname := "db1"

		data := CmdSubstitutions{
			DATABASE:    dbname,
			SERVER_IP:   serverAddress,
			SERVER_PORT: strconv.Itoa(serverPort),
			HOSTNAME:    nodeAddress,
			PORT:        strconv.Itoa(nodePort),
		}
		output, _ := cmd(cmd, data)

		Convey("The command should be equal", func() {
			So(output, ShouldEqual, "add db1 -s node2:1111 -h node1 -p 2222")
		})
	})
}

func TestRun2(t *testing.T) {
	Convey("Creating a command for values", t, func() {
		cmd := "del {{.DATABASE}} -s {{.SERVER_IP}}:{{.SERVER_PORT}} -h {{.HOSTNAME}} -p {{.PORT}}"
		nodeAddress := "node1"
		nodePort := 2222
		serverAddress := "node2"
		serverPort := 1111
		dbname := "db1"

		data := CmdSubstitutions{
			DATABASE:    dbname,
			SERVER_IP:   serverAddress,
			SERVER_PORT: strconv.Itoa(serverPort),
			HOSTNAME:    nodeAddress,
			PORT:        strconv.Itoa(nodePort),
		}
		output, _ := cmd(cmd, data)

		Convey("The command should be equal", func() {
			So(output, ShouldEqual, "del db1 -s node2:1111 -h node1 -p 2222")
		})
	})
}
