# couchdb-manager #

couchdb-manager is a service that monitor chenges on etcd and runs commands to add or remove database syncronization to couchdb clusters

## Command usage ##

With config into etcd:
```
couchdb-manager -etcd="http://192.168.0.10:4001"
```

## Configuration ##

### Etcd directory structure ###

```
/couchdb-mng
  |- /config
      |- /actionName
          |- /config
          |- /databases
              |- DB1
              |- DB2
```

### Action configuration ###

**/couchdb-mng/config/actionName/config** contains couchdb-manager action in json format:

```json
{
    "Key": "/skydns/local/coreos1/docker/deliver",
    "Add": "add -s SERVER_IP:SERVER_PORT -h HOSTNAME -p PORT",
    "Del": "del -s SERVER_IP:SERVER_PORT -h HOSTNAME -p PORT"
}
```

# **Key** etcd path to watch (skydns host)
# **Add** command to run to add a new node
# **Del** command to run to remove a node

#### Variables ####

# **DATABASE** CourchDB database to synchronize
# **SERVER_IP** CourchDB ipaddress to connect
# **SERVER_PORT** CouchDB port to connect
# **HOSTNAME** New CouchDB node's ip address
# **PORT** New CouchDB node's port
