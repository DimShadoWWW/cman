package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/DimShadoWWW/cman/node"
	"github.com/codeskyblue/go-sh"
	"github.com/coreos/go-etcd/etcd"
	"github.com/deckarep/golang-set"
	"github.com/fjl/go-couchdb"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"
)

type Add struct {
	ID         string `json:"_id"`
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

func (a *Add) IdTmpl() string {
	return "{{.HOSTNAME}}_{{.PORT}}_{{.DATABASE}}"
}

func (a *Add) Id(dbname, hostname, port string) string {
	var script bytes.Buffer
	data := CmdSubstitutions{
		DATABASE: dbname,
		HOSTNAME: hostname,
		PORT:     port,
	}

	// Replace Source from data
	templ, err := template.New("replkey").Parse(a.IdTmpl())
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	ctmpl := template.Must(templ, err)
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	err = ctmpl.Execute(&script, data)
	if err != nil {
		log.Println("Failed command template substitution:", err)
	}
	return script.String()
}

type ActionCfg struct {
	Key       string   `json:"Key"`       // Etcd key to look for couchdb nodes
	PutCmd    []string `json:"PutCmd"`    // Command to run to add
	PostCmd   []string `json:"PostCmd"`   // Command to run to add
	DeleteCmd []string `json:"DeleteCmd"` // Command to run to delete
	Add       Add      `json:"Add"`       // Command's params to add replication
	Username  string   `json:"Username"`  // Command's params to add replication
	Password  string   `json:"Password"`  // Command's params to add replication
}

type Actions struct {
	Action    ActionCfg
	Databases mapset.Set     // Active databases
	Nodes     node.NodeSlice // Active Nodes
}

type CmdSubstitutions struct {
	DATABASE    string
	SERVER_IP   string
	SERVER_PORT string
	HOSTNAME    string
	PORT        string
	AUTH        string
}

func CheckOrCreateDB(action Actions, n node.Node) {
	var rt http.RoundTripper
	for dbname := range action.Databases.Iter() {
		log.Println("Checking DB on " + n.Host + ":" + strconv.Itoa(n.Port))

		c, err := couchdb.NewClient("http://"+n.Host+":"+strconv.Itoa(n.Port)+"/", rt)
		if err != nil {
			log.Printf("Connection error: %s -> %v", n.Host+":"+strconv.Itoa(n.Port), err)
		}
		if action.Action.Username != "" {
			c.SetAuth(couchdb.BasicAuth(action.Action.Username, action.Action.Password))
		}
		err = c.Ping() // trigger round trip
		if err != nil {
			log.Printf("Server %s is down: %v", n.Host+":"+strconv.Itoa(n.Port), err)
		} else {
			db, err := c.CreateDB(nodeName(dbname.(*etcd.Node)))
			if err == nil {
				if db.Name() == nodeName(dbname.(*etcd.Node)) {
					log.Println("Database ", db.Name(), " created")
				}
			}
		}
	}
}

// Add replication of dbname from remote node (origHost, origPort) to local node (destHost, destPort)
func AddReplication(action Actions, dbname, origHost, origPort, destHost, destPort string) {
	var sscript, tscript bytes.Buffer

	replication_key := action.Action.Add.Id(dbname, origHost, origPort)

	log.Println("Checking for replication " + replication_key + " on " + destHost + ":" + destPort)

	data := CmdSubstitutions{
		DATABASE:    dbname,
		SERVER_IP:   destHost,
		SERVER_PORT: destPort,
		HOSTNAME:    origHost,
		PORT:        origPort,
	}
	if action.Action.Username != "" {
		data.AUTH = action.Action.Username + ":" + action.Action.Password + "@"
	}

	// Add replication
	replication_data := action.Action.Add

	// Replace Source from data
	stempl, err := template.New("source").Parse(replication_data.Source)
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	stmpl := template.Must(stempl, err)
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	err = stmpl.Execute(&sscript, data)
	if err != nil {
		log.Println("Failed command template substitution:", err)
	}
	replication_data.Source = sscript.String()

	// Replace Source from data
	ttempl, err := template.New("source").Parse(replication_data.Target)
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	ttmpl := template.Must(ttempl, err)
	if err != nil {
		log.Println("Failed to load template from action configuration: ", err)
	}
	err = ttmpl.Execute(&tscript, data)
	if err != nil {
		log.Println("Failed command template substitution:", err)
	}
	// Target can't be just the dbname because of this: http://permalink.gmane.org/gmane.comp.db.couchdb.user/21685
	replication_data.Target = tscript.String()
	// // Target is the local "dbname" in destination server
	// replication_data.Target = dbname
	replication_data.ID = replication_key

	var rt http.RoundTripper

	c, err := couchdb.NewClient("http://"+destHost+":"+destPort+"/", rt)
	if err != nil {
		log.Printf("Connection error: %s -> %v", destHost+":"+destPort, err)
	}
	if action.Action.Username != "" {
		c.SetAuth(couchdb.BasicAuth(action.Action.Username, action.Action.Password))
	}
	err = c.Ping() // trigger round trip
	if err != nil {
		log.Printf("Server %s is down: %v", destHost+":"+destPort, err)
	} else {
		tdata := action.Action.Add
		if err := c.DB("_replicator").Get(replication_key, &tdata, nil); err != nil {
			log.Println("Replication doesn't exist.. adding")
			_, err := c.DB("_replicator").Put(replication_key, replication_data, "")
			if err != nil {
				log.Println(err)
			} else {
				log.Println("Replication added")
			}
		}
	}
}

// Add replication of dbname from remote node (origHost, origPort) to local node (destHost, destPort)
func DelReplication(action Actions, dbname, origHost, origPort, destHost, destPort string) {
	replication_key := action.Action.Add.Id(dbname, origHost, origPort)

	log.Println("Checking for replication " + replication_key + " on " + destHost + ":" + destPort)

	var rt http.RoundTripper

	c, err := couchdb.NewClient("http://"+destHost+":"+destPort+"/", rt)
	if err != nil {
		log.Printf("Connection error: %s -> %v", destHost+":"+destPort, err)
	}
	if action.Action.Username != "" {
		c.SetAuth(couchdb.BasicAuth(action.Action.Username, action.Action.Password))
	}
	err = c.Ping() // trigger round trip
	if err != nil {
		log.Printf("Server %s is down: %v", destHost+":"+destPort, err)
	} else {
		tdata := action.Action.Add
		if err := c.DB("_replicator").Get(replication_key, &tdata, nil); err == nil {
			log.Println("Replication exist.. removing")
			rev, err := c.DB("_replicator").Rev(replication_key)
			if err != nil {
				log.Println(err)
			} else {
				rev, err = c.DB("_replicator").Delete(replication_key, rev)
				if err != nil {
					log.Println(err)
				} else {
					log.Println("Replication removed")
				}
			}
		}
	}
}

func run(cmd []string) (string, error) {
	log.Printf("Executing: %#v\n", cmd)
	out, err := sh.Command(cmd[0], cmd[1:len(cmd)]).SetTimeout(3 * time.Second).Output()
	return string(out), err
}

func cmd(command []string, data CmdSubstitutions) ([]string, error) {
	var comm []string

	for _, cm := range command {
		var script bytes.Buffer

		templ, err := template.New("command").Parse(cm)
		if err != nil {
			log.Println("Failed to load template from action configuration: ", err)
		}

		c := template.Must(templ, err)
		if err != nil {
			log.Println("Failed to load template from action configuration: ", err)
		}

		err = c.Execute(&script, data)
		if err != nil {
			log.Println("Failed command template substitution:", err)
		}

		comm = append(comm, script.String())
	}
	log.Printf("Running: %s\n", comm)
	return comm, nil
}

func processUpdate(r etcd.Response) error {

	log.Println("Processing update")

	var entry node.Node

	configRegexp, err := regexp.Compile(etcdKeyPrefix + "/actions/([a-zA-Z0-9_]+)")
	if err != nil {
		log.Println(err)
		return err
	}

	// databasesRegexp, err := regexp.Compile(etcdKeyPrefix + "/config/([a-zA-Z0-9_]+)/databases/([a-zA-Z0-9_]+)")
	// if err == nil {
	// 	return err
	// }

	log.Println("Checking update")
	// If it was inside the configuration
	if configRegexp.MatchString(r.Node.Key) == true {
		log.Println("Configuration update")
		log.Printf("Update: %#v\n", r.Node)

		matches := configRegexp.FindAllStringSubmatch(r.Node.Key, -1)
		actionName := matches[0][1]

		// Action removed
		if r.Node.Key == etcdKeyPrefix+"/actions/"+actionName && r.Node.Value == "" {
			log.Printf("Action %s removed\n", actionName)
			delete(Registry, actionName)
			return nil
		}

		if actionName != "" {
			// action config
			switch {
			case r.Node.Key == etcdKeyPrefix+"/actions/"+actionName+"/config":

				if _, ok := Registry[actionName]; ok {
					log.Println("Action config update")
					// Load Config
					resp, err := client.Get(etcdKeyPrefix+"/actions/"+actionName+"/config", false, false)
					// Check if it is connected or add the default config if
					if err != nil {
						log.Println("Error: config of action ", etcdKeyPrefix+"/actions/"+actionName+"/config", " doesn't exist")
						return err
					}

					n := ActionCfg{}

					err = json.Unmarshal([]byte(resp.Node.Value), &n)
					if err != nil {
						log.Println("Error: ", err.Error())
						return err
					}

					Registry[actionName].Action = n
				} else {

					// Load Config
					resp, err := client.Get(etcdKeyPrefix+"/actions/"+actionName+"/config", false, false)
					// Check if it is connected or add the default config if
					if err != nil {
						log.Println("Error: config of action ", etcdKeyPrefix+"/actions/"+actionName+"/config", " doesn't exist")
						log.Println(err.Error())
						return err
					}

					n := ActionCfg{}

					err = json.Unmarshal([]byte(resp.Node.Value), &n)
					if err != nil {
						log.Println("Error: config of action ", etcdKeyPrefix+"/actions/"+actionName+"/config", " failed to unmarchal.")
						log.Println(err)
						return err
					}

					// Read databases
					databases := mapset.NewSet()
					resp, err = client.Get(etcdKeyPrefix+"/actions/"+actionName+"/databases", true, true)
					// Check if it is connected or add the default config if
					if err != nil {
						log.Println("Error: there is no databases configured for action ", etcdKeyPrefix+"/actions/"+actionName)
					} else {
						for _, db := range resp.Node.Nodes {
							if db.Dir == false {
								databases.Add(db)
							}
						}
					}

					nodes, err := getNodes(n.Key)
					if err != nil {
						log.Println("Info: there are no hosts for", actionName)
						// continue
					}

					Registry[actionName] = &Actions{
						Action:    n,
						Databases: databases, // Active databases
						Nodes:     nodes,
						// Hosts:     resp.Node.Nodes,
					}
				}

			case strings.Contains(r.Node.Key, etcdKeyPrefix+"/actions/"+actionName+"/databases/"):
				log.Println("Action databases update")
				// Read databases
				resp, err := client.Get(etcdKeyPrefix+"/actions/"+actionName+"/databases", true, true)
				// Check if it is connected or add the default config if
				if err != nil {
					log.Println("Error: there is no databases configured for action ", resp.Node.Key)
				}
				databaseName := nodeName(r.Node)

				var NList node.NodeList

				if _, ok := Registry[actionName]; ok {
					for k := range Registry[actionName].Nodes {
						NList = append(NList, k)
					}

					if r.Node.Value != "" {
						Registry[actionName].Databases.Add(databaseName)

						for comb := range node.Permutations(NList, select_num, buf) {
							lh := Registry[actionName].Nodes[comb[0]] // local CouchDB
							rh := Registry[actionName].Nodes[comb[1]] // remote CouchDB for syncronization
							CheckOrCreateDB(*Registry[actionName], rh)
							AddReplication(*Registry[actionName], databaseName, rh.Host, strconv.Itoa(rh.Port), lh.Host, strconv.Itoa(lh.Port))

							// log.Println("Adding database ", databaseName, " to server ", lh.Host+":"+strconv.Itoa(lh.Port))
							// data := CmdSubstitutions{
							// 	DATABASE:    databaseName,
							// 	SERVER_IP:   lh.Host,
							// 	SERVER_PORT: strconv.Itoa(lh.Port),
							// 	HOSTNAME:    rh.Host,
							// 	PORT:        strconv.Itoa(rh.Port),
							// }

							// // Force DB creation

							// fmt.Println("Checking or creatind remote database ", databaseName, " on ", lh.Host, ":", strconv.Itoa(lh.Port))
							// to_execute := append(Registry[actionName].Action.PutCmd, Registry[actionName].Action.Add.Source)
							// command, err := cmd(to_execute, data)
							// if err != nil {
							// 	log.Printf(r.Node.Value)
							// 	log.Printf(err.Error())
							// }
							// out, err := run(command)
							// if err != nil {
							// 	log.Println("Command failed:")
							// 	log.Println("exec: ", strings.Join(command, " "))
							// 	log.Println(out)
							// 	log.Println(err.Error())
							// } else {
							// 	log.Println(out)
							// }
							// to_execute = append(Registry[actionName].Action.PostCmd, "-d", Registry[actionName].Action.Add.String(), "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/_replicate/"+Registry[actionName].Action.Add.IdTmpl())
							// command, err = cmd(to_execute, data)
							// if err != nil {
							// 	log.Printf(r.Node.Value)
							// 	log.Printf(err.Error())
							// }
							// out, err = run(command)
							// if err != nil {
							// 	log.Println("Command failed:")
							// 	log.Println("exec: ", strings.Join(command, " "))
							// 	log.Println(out)
							// 	log.Println(err.Error())
							// } else {
							// 	log.Println(out)
							// }
						}
					} else {
						Registry[actionName].Databases.Remove(databaseName)

						for comb := range node.Permutations(NList, select_num, buf) {
							lh := Registry[actionName].Nodes[comb[0]] // local CouchDB
							rh := Registry[actionName].Nodes[comb[1]] // remote CouchDB with syncronization

							log.Println("Removing database ", databaseName, " from server ", lh.Host+":"+strconv.Itoa(lh.Port))
							DelReplication(*Registry[actionName], databaseName, rh.Host, strconv.Itoa(rh.Port), lh.Host, strconv.Itoa(lh.Port))

							// data := CmdSubstitutions{
							// 	DATABASE:    databaseName,
							// 	SERVER_IP:   lh.Host,
							// 	SERVER_PORT: strconv.Itoa(lh.Port),
							// 	HOSTNAME:    rh.Host,
							// 	PORT:        strconv.Itoa(rh.Port),
							// }
							// to_execute := append(Registry[actionName].Action.DeleteCmd, "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/_replicate/", Registry[actionName].Action.Add.IdTmpl())
							// command, err := cmd(to_execute, data)
							// if err != nil {
							// 	log.Printf(r.Node.Value)
							// 	log.Printf(err.Error())
							// }
							// out, err := run(command)
							// if err != nil {
							// 	log.Println("Command failed:")
							// 	log.Println("exec: ", strings.Join(command, " "))
							// 	log.Println(out)
							// 	log.Println(err.Error())
							// } else {
							// 	log.Println(out)
							// }
						}
					}
				}
			}
		}
	} else {
		// It was a host update

		// find the action who controls this host change
		log.Println("Hosts update")
		log.Printf("Update: %#v\n", r.Node)
		if len(Registry) > 0 {

			for _, v := range Registry {
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
													for dbname := range v.Databases.Iter() {
														log.Println("Removing node", bn.Host+":"+strconv.Itoa(bn.Port), " from server ", node.Host+":"+strconv.Itoa(node.Port))
														DelReplication(*v, nodeName(dbname.(*etcd.Node)), bn.Host, strconv.Itoa(bn.Port), node.Host, strconv.Itoa(node.Port))

														// data := CmdSubstitutions{
														// 	DATABASE:    nodeName(dbname.(*etcd.Node)),
														// 	SERVER_IP:   node.Host,
														// 	SERVER_PORT: strconv.Itoa(node.Port),
														// 	HOSTNAME:    bn.Host,
														// 	PORT:        strconv.Itoa(bn.Port),
														// }
														// to_execute := append(v.Action.DeleteCmd, "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/_replicate/"+v.Action.Add.IdTmpl())
														// command, err := cmd(to_execute, data)
														// if err != nil {
														// 	log.Printf(r.Node.Value)
														// 	log.Printf(err.Error())
														// }
														// out, err := run(command)
														// if err != nil {
														// 	log.Println("Command failed:")
														// 	log.Println("exec: ", strings.Join(command, " "))
														// 	log.Println(out)
														// 	log.Println(err.Error())
														// } else {
														// 	log.Println(out)
														// }
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
							err := json.Unmarshal([]byte(r.Node.Value), &entry)
							if err != nil {
								log.Printf(r.Node.Value)
								log.Printf(err.Error())
								log.Println("Failed to read json '", r.Node.Value, "' error: ", err.Error())
							} else {
								log.Println("Databases Checked")
								if len(nodes) > 1 {
									for _, node := range nodes {
										for _, bn := range v.Nodes {
											if bn.Host != node.Host || bn.Port != node.Port {
												log.Printf("node1: %#v\n", bn)
												log.Printf("node2: %#v\n", node)
												CheckOrCreateDB(*v, bn)
												CheckOrCreateDB(*v, node)

												for dbname := range v.Databases.Iter() {
													// direct
													AddReplication(*v, nodeName(dbname.(*etcd.Node)), bn.Host, strconv.Itoa(bn.Port), node.Host, strconv.Itoa(node.Port))
													// reverse
													AddReplication(*v, nodeName(dbname.(*etcd.Node)), node.Host, strconv.Itoa(node.Port), bn.Host, strconv.Itoa(bn.Port))

													// // Force DB creation
													// fmt.Printf("dbname %#v\n", dbname)
													// fmt.Printf("nodeName(dbname.(*etcd.Node)) %#v\n", nodeName(dbname.(*etcd.Node)))
													// d := CmdSubstitutions{
													// 	DATABASE:    nodeName(dbname.(*etcd.Node)),
													// 	SERVER_IP:   node.Host,
													// 	SERVER_PORT: strconv.Itoa(node.Port),
													// }

													// fmt.Println("Checking or creating remote database ", nodeName(dbname.(*etcd.Node)), " on ", entry.Host, ":", strconv.Itoa(entry.Port))

													// to_execute := append(v.Action.PutCmd, "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/{{.DATABASE}}")
													// command, err := cmd(to_execute, d)
													// if err != nil {
													// 	log.Printf(r.Node.Value)
													// 	log.Printf(err.Error())
													// }
													// out, err := run(command)
													// if err == nil {
													// 	log.Println("Database created")
													// }

													// // synchronization
													// log.Println("Adding node", bn.Host+":"+strconv.Itoa(bn.Port), " to server ", node.Host+":"+strconv.Itoa(node.Port))
													// data := CmdSubstitutions{
													// 	DATABASE:    nodeName(dbname.(*etcd.Node)),
													// 	SERVER_IP:   node.Host,
													// 	SERVER_PORT: strconv.Itoa(node.Port),
													// 	HOSTNAME:    bn.Host,
													// 	PORT:        strconv.Itoa(bn.Port),
													// }
													// to_execute = append(v.Action.PostCmd, "-d", v.Action.Add.String(), "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/_replicate/"+v.Action.Add.IdTmpl())
													// command, err = cmd(to_execute, data)
													// if err != nil {
													// 	log.Printf(r.Node.Value)
													// 	log.Printf(err.Error())
													// }
													// out, err = run(command)
													// if err != nil {
													// 	log.Println("Command failed:")
													// 	log.Println("exec: ", strings.Join(command, " "))
													// 	log.Println(out)
													// 	log.Println(err.Error())
													// } else {
													// 	log.Println(out)
													// }

													// // reverse
													// log.Println("Adding node", bn.Host+":"+strconv.Itoa(bn.Port), " to server ", node.Host+":"+strconv.Itoa(node.Port))
													// data = CmdSubstitutions{
													// 	DATABASE:    nodeName(dbname.(*etcd.Node)),
													// 	SERVER_IP:   bn.Host,
													// 	SERVER_PORT: strconv.Itoa(bn.Port),
													// 	HOSTNAME:    node.Host,
													// 	PORT:        strconv.Itoa(node.Port),
													// }
													// to_execute = append(v.Action.PostCmd, "-d", v.Action.Add.String(), "http://{{.SERVER_IP}}:{{.SERVER_PORT}}/_replicate/"+v.Action.Add.IdTmpl())
													// command, err = cmd(to_execute, data)
													// if err != nil {
													// 	log.Printf(r.Node.Value)
													// 	log.Printf(err.Error())
													// }
													// out, err = run(command)
													// if err != nil {
													// 	log.Println("Command failed:")
													// 	log.Println("exec: ", strings.Join(command, " "))
													// 	log.Println(out)
													// 	log.Println(err.Error())
													// } else {
													// 	log.Println(out)
													// }
												}
											}
										}
									}
								}

								if _, ok := v.Nodes[r.Node.Key]; !ok {
									v.Nodes = make(node.NodeSlice)
								}
								v.Nodes[r.Node.Key] = node.Node{Port: entry.Port, Host: entry.Host, Key: r.Node.Key}
							}
							// Registry = make(map[string]*Actions)
							// mm, ok := v.Nodes[r.Node.Key]
							// if !ok {
							// 	mm = make(map[string]int)
							// 	m[path] = mm
							// }
						}
					} else {
						log.Println("Error retrieving node list for key '", parentNodeKey(r.Node.Key), "': ", err.Error())
						continue
					}
				}
			}
		} else {
			log.Printf("Registry: %#v\n", Registry)
			log.Println("There is no active actions for hosts update")
		}
	}

	return nil
}

func getNodes(etcdPath string) (node.NodeSlice, error) {
	nodes := make(node.NodeSlice)

	etcdnode, err := client.Get(etcdPath, true, true)
	if err != nil {
		if err.(*etcd.EtcdError).ErrorCode == etcd.ErrCodeEtcdNotReachable {
			log.Println("Reconnecting to etcd ", etcdUrl)
			client = etcd.NewClient([]string{etcdUrl})
		}
		// log.Println(err)
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

	for _, action := range resp.Node.Nodes {
		// Action has to be a folder

		if action.Dir == true {

			// Load Config
			resp, err := client.Get(action.Key+"/config", false, false)
			// Check if it is connected or add the default config if
			if err != nil {
				log.Println("Error: config of action ", action.Key+"/config", " doesn't exist")
				log.Println(err.Error())
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
			databases := mapset.NewSet()
			resp, err = client.Get(action.Key+"/databases", true, true)
			// Check if it is connected or add the default config if
			if err != nil {
				log.Println("Error: there is no databases configured for action ", action.Key)
			} else {
				for _, db := range resp.Node.Nodes {
					if db.Dir == false {
						databases.Add(db)
					}
				}
			}

			nodes, err := getNodes(n.Key)
			if err != nil {
				log.Println("Info: there are no hosts for", nodeName(action))
				// continue
			}

			Registry[nodeName(action)] = &Actions{
				Action:    n,
				Databases: databases, // Active databases
				Nodes:     nodes,
				// Hosts:     resp.Node.Nodes,
			}
		}
	}
	log.Println("Configuration loaded ..")
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
	Registry      map[string]*Actions
)

const (
	// Permutation parameters
	select_num = 2
	buf        = 5
)

func main() {
	Registry = make(map[string]*Actions)

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

	log.Println("Loading Configuration ..")
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
		log.Println(err)
		errCh <- err
	}()
	for {
		select {
		case resp := <-receiver:
			if resp != nil {
				// log.Println("resp: ", resp)
				err := processUpdate(*resp)
				if err != nil {
					log.Println("processUpdate failed:", err)
				}
			} else {
				<-errCh
			}
		case err := <-errCh:
			fmt.Println(err)
		}
	}
}
