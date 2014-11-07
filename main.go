package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/DimShadoWWW/cman/node"
	"github.com/coreos/go-etcd/etcd"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
)

type ActionCfg struct {
	Key    string
	Add    string
	Del    string
	Create string
	Remove string
}

type Actions struct {
	Action    ActionCfg
	Databases []string       // Active databases
	Nodes     node.NodeSlice // Active Nodes
}

func CouchDBCreate(dbname string, nodeAddress string, nodePort int, serverAddress string, serverPort int) error {
	c, err := couchbase.Connect("http://" + nodeAddress + ":" + strconv.Itoa(nodePort) + "/")
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket("dbname")
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

}

func run(command string, dbname string, nodeAddress string, nodePort int, serverAddress string, serverPort int) (string, error) {
	cmd := strings.Replace(
		strings.Replace(
			strings.Replace(
				strings.Replace(
					strings.Replace(
						command,
						"DATABASE", dbname, -1),
					"SERVER_IP", serverAddress, -1),
				"SERVER_PORT", strconv.Itoa(serverPort), -1),
			"HOSTNAME", nodeAddress, -1),
		"PORT", strconv.Itoa(nodePort), -1)
	log.Printf("Running: %s\n", cmd)
	return cmd, nil
}

func processUpdate(r etcd.Response) error {

	log.Println("Processing update")

	var entry node.Node

	configRegexp, err := regexp.Compile(etcdKeyPrefix + "/config/([a-zA-Z0-9_]+)")
	if err == nil {
		return err
	}

	databasesRegexp, err := regexp.Compile(etcdKeyPrefix + "/config/([a-zA-Z0-9_]+)/databases/([a-zA-Z0-9_]+)")
	if err == nil {
		return err
	}

	// If it was inside the configuration
	if configRegexp.MatchString(r.Node.Key) == true {
		log.Println("Configuration update")
		log.Printf("Update: %#v\n", r.Node)

		matches := configRegexp.FindAllStringSubmatch(r.Node.Key, -1)
		actionName := matches[0][1]

		// Action removed
		if r.Node.Key == etcdKeyPrefix+"/config/"+actionName && r.Node.Value == "" {
			log.Printf("Action %s removed\n", actionName)
			delete(Registry, actionName)
			return nil
		}

		if actionName != "" {
			// action config
			switch {
			case r.Node.Key == etcdKeyPrefix+"/config/"+actionName+"/config":
				// Load Config
				resp, err := client.Get(etcdKeyPrefix+"/config/"+actionName+"/config", false, false)
				// Check if it is connected or add the default config if
				if err != nil {
					log.Println("Error: config of action ", etcdKeyPrefix+"/config/"+actionName+"/config", " doesn't exist")
					return err
				}

				n := ActionCfg{}

				err = json.Unmarshal([]byte(resp.Node.Value), &n)
				if err != nil {
					log.Println("Error: config of action ", action.Key+"/config", " failed to unmarchal.")
					return err
				}

				Registry[actionName].Action = n
			case strings.Contains(r.Node.Key, etcdKeyPrefix+"/config/"+actionName+"/databases/"):
				// Read databases
				resp, err = client.Get(action.Key+"/databases", true, true)
				// Check if it is connected or add the default config if
				if err != nil {
					log.Println("Error: there is no databases configured for action ", action.Key)
					continue
				}
				var databases []string
				for _, db := range resp.Node.Nodes {
					if db.Dir == false {
						databases = append(databases, nodeName(db))
					}
				}

				Registry[actionName] = Actions{
					Databases: databases, // Active databases
				}

				databaseName := nodeName(r.Node.Key)

				if r.Node.Value == "" {
					for _, h := range Registry[actionName].Nodes {
						log.Println("Removing database ", databaseName, " from server ", node.Host+":"+strconv.Itoa(node.Port))
						cmd, err := run(v.Action.Del, dbname, bn.Host, bn.Port, node.Host, node.Port)
						if err != nil {
							log.Printf(r.Node.Value)
							log.Printf(err.Error())
						}
					}
				}

			}
			// if strings.Contains(r.Node.Key, etcdKeyPrefix+"/config/"+actionName+"/config") {
			// 	nodes, err := getNodes(v.Action.Key)
			// 	if err == nil {
			// 		// keeping node list before the update to compare
			// 		beforeNodes := make(node.NodeSlice)
			// 		for kn, vn := range v.Nodes {
			// 			beforeNodes[kn] = vn
			// 		}

			// 		if r.Node.Value == "" {
			// 			if v.Nodes.HasKey(r.Node.Key) {
			// 				delete(v.Nodes, r.Node.Key)
			// 				log.Printf("Removed: %s\n", r.Node.Key)
			// 				log.Printf("Left nodes: %v\n", v.Nodes)
			// 				if len(beforeNodes) > 1 {
			// 					for _, bn := range beforeNodes {
			// 						if bn.Key == r.Node.Key {
			// 							for _, node := range v.Nodes {
			// 								if bn.Host != node.Host || bn.Port != node.Port {
			// 									for _, dbname := range v.Databases {
			// 										log.Println("Removing node", bn.Host+":"+strconv.Itoa(bn.Port), " from server ", node.Host+":"+strconv.Itoa(node.Port))
			// 										cmd, err := run(v.Action.Del, dbname, bn.Host, bn.Port, node.Host, node.Port)
			// 										if err != nil {
			// 											log.Printf(r.Node.Value)
			// 											log.Printf(err.Error())
			// 										}
			// 										log.Println(cmd)
			// 									}
			// 								}
			// 							}
			// 							break
			// 						}
			// 					}
			// 				}
			// 			} else {
			// 				log.Println("Slice hasn't that key")
			// 			}
			// 		} else {
			// 			log.Printf("Adding: %s\n", r.Node.Value)
			// 			if len(nodes) > 1 {
			// 				for _, node := range nodes {
			// 					for _, bn := range v.Nodes {
			// 						if bn.Host != node.Host || bn.Port != node.Port {
			// 							for _, dbname := range v.Databases {
			// 								log.Println("Adding node", bn.Host+":"+strconv.Itoa(bn.Port), " to server ", node.Host+":"+strconv.Itoa(node.Port))
			// 								cmd, err := run(v.Action.Add, dbname, bn.Host, bn.Port, node.Host, node.Port)
			// 								if err != nil {
			// 									log.Printf(r.Node.Value)
			// 									log.Printf(err.Error())
			// 									panic(err)
			// 								}
			// 								log.Println(cmd)
			// 							}
			// 						}
			// 					}
			// 				}
			// 			}
			// 			err := json.Unmarshal([]byte(r.Node.Value), &entry)
			// 			if err != nil {
			// 				log.Println("Failed to read json '", r.Node.Value, "' error: ", err.Error())
			// 			}
			// 			v.Nodes[r.Node.Key] = node.Node{Port: entry.Port, Host: entry.Host, Key: r.Node.Key}
			// 		}
			// 	} else {
			// 		log.Println("Error retrieving node list for key '", parentNodeKey(r.Node.Key), "': ", err.Error())
			// 		continue
			// 	}
			// }
		}
	} else {
		// It was a host update

		// find the action who controls this host change
		log.Println("Hosts update")
		log.Printf("Update: %#v\n", r.Node)
		if len(Registry) > 0 {
			for k, v := range Registry {
				log.Printf("registry: {%s : %#v}\n", k, v)
				if strings.Contains(r.Node.Key, v.Action.Key) {
					nodes, err := getNodes(v.Action.Key)
					if err == nil {
						// keeping node list before the update to compare
						beforeNodes := make(node.NodeSlice)
						for kn, vn := range v.Nodes {
							beforeNodes[kn] = vn
						}

						if r.Node.Value == "" {
							if v.Nodes.HasKey(r.Node.Key) {
								delete(v.Nodes, r.Node.Key)
								log.Printf("Removed: %s\n", r.Node.Key)
								log.Printf("Left nodes: %v\n", v.Nodes)
								if len(beforeNodes) > 1 {
									for _, bn := range beforeNodes {
										if bn.Key == r.Node.Key {
											for _, node := range v.Nodes {
												if bn.Host != node.Host || bn.Port != node.Port {
													for _, dbname := range v.Databases {
														log.Println("Removing node", bn.Host+":"+strconv.Itoa(bn.Port), " from server ", node.Host+":"+strconv.Itoa(node.Port))
														cmd, err := run(v.Action.Del, dbname, bn.Host, bn.Port, node.Host, node.Port)
														if err != nil {
															log.Printf(r.Node.Value)
															log.Printf(err.Error())
														}
														log.Println(cmd)
													}
												}
											}
											break
										}
									}
								}
							} else {
								log.Println("Slice hasn't that key")
							}
						} else {
							log.Printf("Adding: %s\n", r.Node.Value)
							if len(nodes) > 1 {
								for _, node := range nodes {
									for _, bn := range v.Nodes {
										if bn.Host != node.Host || bn.Port != node.Port {
											for _, dbname := range v.Databases {
												log.Println("Adding node", bn.Host+":"+strconv.Itoa(bn.Port), " to server ", node.Host+":"+strconv.Itoa(node.Port))
												cmd, err := run(v.Action.Add, dbname, bn.Host, bn.Port, node.Host, node.Port)
												if err != nil {
													log.Printf(r.Node.Value)
													log.Printf(err.Error())
													panic(err)
												}
												log.Println(cmd)
											}
										}
									}
								}
							}
							err := json.Unmarshal([]byte(r.Node.Value), &entry)
							if err != nil {
								log.Println("Failed to read json '", r.Node.Value, "' error: ", err.Error())
							}
							v.Nodes[r.Node.Key] = node.Node{Port: entry.Port, Host: entry.Host, Key: r.Node.Key}
						}
					} else {
						log.Println("Error retrieving node list for key '", parentNodeKey(r.Node.Key), "': ", err.Error())
						continue
					}
				}
			}
		} else {
			log.Println("There is no active actions for hosts update")
		}
	}
}

func getNodes(etcdPath string) (node.NodeSlice, error) {
	nodes := node.NodeSlice{}

	etcdnode, err := client.Get(etcdPath, true, true)
	if err != nil {
		if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
			log.Println("Reconnecting to etcd ", etcdUrl)
			client = etcd.NewClient([]string{etcdUrl})
		}
		log.Println(err)
		return nil, err
	}

	log.Printf("Analyzing Node %#v\n", etcdnode.Node)
	for _, nod := range etcdnode.Node.Nodes {
		n := node.Node{}
		err = json.Unmarshal([]byte(nod.Value), &n)
		if err != nil {
			continue
		}
		nodes[nod.Key] = node.Node{Port: n.Port, Host: n.Host, Key: nod.Key}
	}
	return nodes, nil
}

func parentNodeKey(path string) string {
	if path == "/" {
		return path
	}
	c := strings.Split(path, "/")
	res := strings.Join(c[:len(c)-1], "/")
	if res == "" {
		res = "/"
	}
	return res
}

func nodeName(n *etcd.Node) string {
	info := strings.Split(n.Key, "/")
	nodeName := ""
	if len(info) > 0 {
		nodeName = info[len(info)-1]
	}
	if nodeName == "" && len(info) > 1 {
		nodeName = info[len(info)-2]
	}
	return nodeName
}

func loadConfig() {
	var resp *etcd.Response
	resp, err := client.Get(etcdKeyPrefix+"/actions", false, false)
	if err != nil {
		if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
			log.Println("Reconnecting to etcd ", etcdUrl)
		}
	}

	log.Printf("%#v\n", resp.Node)
	for _, action := range resp.Node.Nodes {
		// Action has to be a folder
		if action.Dir == true {

			// Load Config
			resp, err := client.Get(action.Key+"/config", false, false)
			// Check if it is connected or add the default config if
			if err != nil {
				log.Println("Error: config of action ", action.Key+"/config", " doesn't exist")
				continue
			}

			n := ActionCfg{}

			err = json.Unmarshal([]byte(resp.Node.Value), &n)
			if err != nil {
				log.Println("Error: config of action ", action.Key+"/config", " failed to unmarchal.")
				log.Println(err)
				continue
			}

			// Read databases
			resp, err = client.Get(action.Key+"/databases", true, true)
			// Check if it is connected or add the default config if
			if err != nil {
				log.Println("Error: there is no databases configured for action ", action.Key)
				continue
			}
			var databases []string
			for _, db := range resp.Node.Nodes {
				if db.Dir == false {
					databases = append(databases, nodeName(db))
				}
			}

			// Read databases
			resp, err = client.Get(n.Key, false, true)
			// Check if it is connected or add the default config if
			if err != nil {
				log.Println("Error: config of action ", action.Key, " doesn't exist")
				continue
			}

			nodes, err := getNodes(n.Key)
			if err != nil {
				log.Println("Error: failed to get nodes from ", n.Key)
				continue
			}

			Registry[nodeName(action)] = Actions{
				Action:    n,
				Databases: databases, // Active databases
				Nodes:     nodes,
				// Hosts:     resp.Node.Nodes,
			}
		}
	}
}

func addAction(name, config string) error {
	for {
		_, err := client.Get(etcdKeyPrefix+"/actions/"+name+"/config", false, false)
		if err != nil {
			if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
				log.Println("Reconnecting to etcd ", etcdUrl)
			} else {
				// If it doesn't exists
				if err.(*etcd.EtcdError).ErrorCode == 100 {
					_, err = client.Set(etcdKeyPrefix+"/actions/"+name+"/config", config, 0)
					if err == nil {
						return nil
					}
				}
			}
		} else {
			return nil
		}
	}
}

func addDatabase(name, dbname, config string) error {
	for {
		_, err := client.Get(etcdKeyPrefix+"/actions/"+name+"/databases/"+dbname, false, false)
		if err != nil {
			if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
				log.Println("Reconnecting to etcd ", etcdUrl)
				// conn
			} else {
				// If it doesn't exists
				if err.(*etcd.EtcdError).ErrorCode == 100 {
					_, err = client.CreateDir(etcdKeyPrefix+"/actions/"+name+"/databases/"+dbname, 0)
					if err == nil {
						return nil
					}
				}
			}
		} else {
			return nil
		}
	}
}

var (
	etcdUrl       string
	configFile    string
	etcdKeyPrefix string
	client        *etcd.Client
	WatchChan     chan *etcd.Response
	Registry      map[string]Actions
)

const (
	// Permutation parameters
	select_num = 2
	buf        = 5
)

func main() {
	Registry = make(map[string]Actions)

	flag.StringVar(&etcdUrl, "etcd", "http://127.0.0.1:4001", "Etcd url")
	flag.StringVar(&etcdKeyPrefix, "etcd-key-prefix", "/couchdb-mng", "Keyspace for data in etcd")
	flag.Parse()

	log.Println("Starting ...")
	client = etcd.NewClient([]string{etcdUrl})

	for {
		_, err := client.Get(etcdKeyPrefix+"/actions", false, false)
		// Check if it is connected or add the default config if
		if err != nil {
			if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
				log.Println("Reconnecting to etcd ", etcdUrl)
			} else {
				// If it doesn't exists
				if err.(*etcd.EtcdError).ErrorCode == 100 {
					log.Println("Etcd config is not there, adding ...")
					_, err = client.CreateDir(etcdKeyPrefix+"/actions", 0)
					if err == nil {
						// Default config was set
						break
					} else {
						log.Printf("%#v\n", err)
					}
				}
			}
		} else {
			break
		}
	}

	loadConfig()

	recursive := true
	index := 0

	log.Println("Starting to watch / ..")

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	stop := make(chan bool)
	go func() {
		<-sigch
		os.Exit(0)
	}()
	receiver := make(chan *etcd.Response)
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Watch("/", uint64(index), recursive, receiver, stop)
		errCh <- err
	}()
	for {
		select {
		case resp := <-receiver:
			processUpdate(*resp)
		case err := <-errCh:
			fmt.Println(err)
		}
	}
}