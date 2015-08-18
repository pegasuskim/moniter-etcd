package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	//"log"
)

type Container struct {
	MasterData json.RawMessage `json:"masterData"`
	SlaveData  json.RawMessage `json:"slaveData"`
}

type DBinfo struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func getConfig(dbtype string) DBinfo {
	//Create the Json string
	//var b = []byte(`{"type": "person", "data":{"id": 12423434, "Name": "Fernando"}}`)
	confBytes, err := ioutil.ReadFile("config.json")

	var container Container
	err = json.Unmarshal(confBytes, &container)
	if err != nil {
		fmt.Printf("error:", err)
	}

	var info DBinfo
	switch dbtype {
	case "master":
		json.Unmarshal(container.MasterData, &info)
		//unmarshal the struct to json
		//result, _ = json.Marshal(f)
	case "slave":
		json.Unmarshal(container.SlaveData, &info)
	default:
		fmt.Printf("dbtype error")
	}
	return info
}

//===========================
type Configure struct {
	AppName string
	Host    string
	Port    int
}

type ServerInfo struct {
	AppName      string
	Host         string
	Port         int
	AccessString []string
}

func GetServerInfo() ServerInfo {
	confBytes, err := ioutil.ReadFile("etcd_config.json")

	if err != nil {
		panic(err)
	}
	var configure Configure

	err = json.Unmarshal(confBytes, &configure)
	if err != nil {
		panic(err)
	}

	accessString := fmt.Sprintf("http://%s:%d", configure.Host, configure.Port)
	var serverInfo ServerInfo
	serverInfo.Host = configure.Host
	serverInfo.Port = configure.Port
	serverInfo.AppName = configure.AppName

	serverInfo.AccessString = []string{accessString}

	//log.Printf("ETCD Information->>>>  : %v", serverInfo)
	return serverInfo
}

/*
func main() {
	fmt.Printf("initialization......")
	ret := getConfig("master")
	fmt.Printf("DB host Data:%s Port:%d\n", ret.Host, ret.Port)
}
*/
