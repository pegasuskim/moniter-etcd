package main

import (
	"./config"
	"encoding/json"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/xuyu/goredis"
	"log"
	"strings"
	"time"
)

type Event string

func NewEvent() Event                   { return "" }
func (e Event) Merge(other Event) Event { return e + other }

func etcd_GetMasterData() (*etcd.Response, *etcd.Client) {
	info := config.GetServerInfo()
	log.Printf("Master info : %s", info.AccessString)

	//key := "SP/Master"
	key := fmt.Sprintf("/%s/Master", info.AppName)
	log.Printf("Master info key : %s\n", key)

	var client *etcd.Client = nil

	client = etcd.NewClient(info.AccessString)
	resp, err := client.Get(key, false, false)

	if err != nil {
		fmt.Println("client.Get")
		log.Fatal(err)
	}

	log.Printf("Current Get information: %s: %s\n", resp.Node.Key, resp.Node.Value)
	return resp, client
}

func etcd_GetSlaveData() (*etcd.Response, *etcd.Client) {
	info := config.GetServerInfo()
	log.Printf("Slave info : %s", info.AccessString)

	//key := "/SP/Slaves"
	key := fmt.Sprintf("%s/Slaves", info.AppName)
	log.Printf("Slave info key : %s\n", key)

	var client *etcd.Client = nil

	client = etcd.NewClient(info.AccessString)

	resp, err := client.Get(key, false, false) //상태 좋은순으로 정렬해서 리턴??

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Current Get information: %s: %s\n", resp.Node.Key, resp.Node.Value)
	return resp, client

}

func etcd_SetData(key string, value string) {
	info := config.GetServerInfo()
	log.Printf("info : %s\n", info.AccessString)

	var client *etcd.Client = nil
	client = etcd.NewClient(info.AccessString)

	_, err := client.Set(key, value, 0)

	if err != nil {
		log.Fatal(err)
		log.Printf("etcd_Setdata ERROR")
	}

	log.Printf("Current Set information: %s\n", value)
}

func etcd_GetDeadData() (*etcd.Response, *etcd.Client) {
	info := config.GetServerInfo()
	log.Printf("Dead info : %s", info.AccessString)

	//key := "SP/Dead"
	key := fmt.Sprintf("%s/Dead", info.AppName)
	log.Printf("Dead list info key : %s\n", key)

	var client *etcd.Client = nil
	client = etcd.NewClient(info.AccessString)

	resp, err := client.Get(key, false, false)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Current Get Dead information: %s: %s\n", resp.Node.Key, resp.Node.Value)
	return resp, client
}

func etcd_GetDeadKeyDelete() {
	info := config.GetServerInfo()
	log.Printf("Dead info : %s", info.AccessString)

	//key := "SP/Dead"
	key := fmt.Sprintf("%s/Dead", info.AppName)
	log.Printf("Dead list info key : %s\n", key)

	var client *etcd.Client = nil
	client = etcd.NewClient(info.AccessString)

	resp, err := client.Delete(key, true)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf(">>>Dead Key information>>>%s %s", resp.Node.Key, resp.Node.Value)
}

func redisHeartBeatchecker(host string) (*goredis.Redis, error) {

	client, err := goredis.Dial(&goredis.DialConfig{Address: host})
	if err != nil {
		log.Printf("Error connection to database %s", host)
		log.Printf("Error connection to database->> %s", err.Error())
		return client, err
	}
	if err := client.Ping(); err != nil {
		log.Printf("FALSE Ping check to database")
		return client, err
	}
	client.ClosePool()
	return client, nil
}

func setSlaveOf(host string) error {

	client, err := goredis.Dial(&goredis.DialConfig{Address: host})
	if err != nil {
		log.Printf("Error SlaveOf connection to database : %s", host)
		return err
	}

	redis_host := strings.Split(host, ":")

	if ret := client.SlaveOf(redis_host[0], redis_host[1]); ret != nil {
		log.Printf("SlaveOf Error %s", host)
		return ret
	}

	log.Printf("OK SlaveOf->> %s", host)
	client.ClosePool()
	return nil
}

func coalesceMessage(in <-chan Event, out chan<- Event) {
	event := NewEvent()
	for e := range in {
		event = event.Merge(e)
	loop:
		for {
			select {
			case e := <-in:
				event = event.Merge(e)
			case out <- event:
				event = NewEvent()
				break loop
			}
		}
	}
}

func receive(in <-chan Event, quit chan<- bool) {

	for {
		if "MasterConnectErr" == <-in {
			time.Sleep(1000 * time.Millisecond)

			old_resp, _ := etcd_GetMasterData() //현재 마스터 노드
			old_host := old_resp.Node.Value

			deadData, _ := etcd_GetDeadData() //죽은 마스터 노드(현재 죽은 마스터 노드)
			deadHost := deadData.Node.Value
			log.Printf("dead host-> %s\n", deadHost)

			resp, _ := etcd_GetSlaveData()
			log.Printf("Slave Of Data-> %s\n", resp.Node.Value)

			var slaves []string
			_ = json.Unmarshal([]byte(resp.Node.Value), &slaves)

			var err error
			for i, _ := range slaves {
				_host := strings.Split(slaves[i], "\n")
				_, err = redisHeartBeatchecker(_host[0])
				if err != nil {
					//connectErr
					log.Printf("Heart Beat Checker ERROR %s", _host[0])
					continue
				}

				//log.Printf("etcd_SetData->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

				err = setSlaveOf(_host[0]) //죽은 마스터 노드와 슬레이브 노드 slaveof 시키기
				if err != nil {
					log.Printf("set Slave Of ERROR %s", _host[0])
					//log.Fatal(err)
					continue
				}
				log.Printf("#1 etcd_SetData Master-> %s", _host[0])
				etcd_SetData("SP/Master", slaves[i])

				var new_slaves []string
				_ = json.Unmarshal([]byte(resp.Node.Value), &new_slaves)
				log.Printf("#2 Old Master >>>>>>>>>>: %s", old_host)

				value := make([]string, len(new_slaves))
				log.Printf("#3 >>> deadMessage etcd_GetSlaveData %s >>>", value)
				log.Printf("#4 >>> deadMessage etcd_GetSlaveData Lenth %d >>>", len(new_slaves))

				if len(new_slaves) > 1 {
					for j, node := range new_slaves { //기존 slaves 복사(마스터로 올라간 놈 빼고)
						if node != old_host {
							value[j] = node
						}
					}

					//value[len(new_slaves)-1] = old_host 	// 죽은놈 넣어주면 안될꺼 같아서 주석처리(etcd에서 slave 데이터를 요청했는데 죽은놈이 껴있으면 이상하니까)
					value[len(new_slaves)-1] = ""
				} else {
					//value[0] = old_host					// 죽은놈 넣어주면 안될꺼 같아서 주석처리
					value[0] = ""
				}
				res, _ := json.Marshal(value)
				log.Printf("Set Slave list  %s", string(res))

				info := config.GetServerInfo()
				key := fmt.Sprintf("%s/Slaves", info.AppName)
				etcd_SetData(key, string(res))

				// resource := make(chan Event)
				// output := make(chan Event)
				// requit := make(chan bool)
				// retcd_chan := make(chan *etcd.Response)

				// go watchServer(resp.Node.Value, resource, requit, retcd_chan)
				// go coalesceMessage(resource, output)
				// go receive(output, quit)

				if deadHost != "" {
					fmt.Printf(">>>>>>>>> dead Host Watch start >>>>>>>>>>")
					deadSource := make(chan Event)
					deadOut := make(chan Event)
					deadQuit := make(chan bool)
					_host := strings.Split(deadHost, "\n")
					fmt.Printf("dead Host Watch start....%s\n", _host[0])

					go DeadHostWatch(_host[0], deadSource, deadQuit)
					go deadCoalesceMessage(deadSource, deadOut)
					go deadMessageReceive(deadOut, deadQuit)
				}

			}

		}
	}
}

func watchServer(out chan<- Event, quit <-chan bool, watch chan *etcd.Response) {

	for {
		select {
		case <-quit:
			log.Printf(" WatchServer Quit Message..........")
			//close(out)
		default:
			time.Sleep(1000 * time.Millisecond)
			resp, _ := etcd_GetMasterData()
			_host := strings.Split(resp.Node.Value, "\n")

			log.Printf(" >>> WatchServer start >>> %s\n", resp.Node.Value)

			client, err := goredis.Dial(&goredis.DialConfig{Address: _host[0]})
			if err != nil {
				info := config.GetServerInfo()
				key := fmt.Sprintf("%s/Dead", info.AppName)
				etcd_SetData(key, _host[0])

				e := Event("MasterConnectErr")
				out <- e
				log.Printf("Error connection to database %s", err.Error())
				continue
			}
			if err := client.Ping(); err != nil {

				info := config.GetServerInfo()
				key := fmt.Sprintf("%s/Dead", info.AppName)
				etcd_SetData(key, _host[0])

				e := Event("MasterConnectErr")
				out <- e
				log.Printf("FALSE Ping check to database")
				continue
			}
			client.ClosePool()
		}
	}
}

func receiveEvent(watchChan chan *etcd.Response) {
	recv := <-watchChan
	log.Printf(">>>>Waiting for an update Action>>>> %v\n", recv)
	//close(watchChan)
}

func DeadHostWatch(host string, deadOut chan<- Event, deadQuit <-chan bool) {
	for {
		select {
		case <-deadQuit:
			fmt.Println("Dead Host Watch Quit Message.......")
			close(deadOut)
			return
		default:
			time.Sleep(1000 * time.Millisecond)
			fmt.Printf("Dead Host Watch check....%s\n", host)

			client, err := goredis.Dial(&goredis.DialConfig{Address: host})
			if err != nil {
				log.Printf("Error connection to database %s", err.Error())
				continue
			}
			log.Printf("Dead Host Ping check to database %s", host)
			if err := client.Ping(); err != nil {
				log.Printf("FALSE Ping check to database")
				continue
			}

			e := Event("DeadHostConnectOk")
			deadOut <- e
			client.ClosePool()
		}
	}
}

func deadCoalesceMessage(in <-chan Event, out chan<- Event) {
	event := NewEvent()
	for e := range in {
		event = event.Merge(e)
	loop:
		for {

			select {
			case e := <-in:
				event = event.Merge(e)
			case out <- event:
				event = NewEvent()
				break loop
			}
		}
	}
}

func deadMessageReceive(deadIn <-chan Event, deadQuit chan<- bool) {
	for {
		time.Sleep(5000 * time.Millisecond)

		if "DeadHostConnectOk" == <-deadIn {
			log.Printf("EVENT Dead Host Connect Ok")

			deadQuit <- true
			time.Sleep(1000 * time.Millisecond)

			deadData, _ := etcd_GetDeadData()
			deadHost := deadData.Node.Value
			_host := strings.Split(deadHost, "\n")
			log.Printf(">>> deadHost!!!! %s >>>", _host)

			// err := setSlaveOf(_host[0])							//죽은 예전 마스터가 살아나도 바로 slaveof안하게 주석 처리
			// if err != nil {										//(죽었다 살아났을떄 데이터 동기화를 하고 slaveof를 해야 하는데 요놈은 바로 slaveof를 한다)
			// 	log.Printf("dead setSlaveOf ERROR %s", deadHost)
			// 	continue
			// }

			resp, _ := etcd_GetSlaveData()
			var slaves []string
			_ = json.Unmarshal([]byte(resp.Node.Value), &slaves)

			value := make([]string, len(slaves)+1)
			log.Printf(">>> deadMessage etcd_GetSlaveData %s >>>", value)
			value[len(slaves)] = deadHost
			// if len(slaves) > 1 {
			// 	value[len(slaves)-1] = deadHost //또 마지막 old_host를 대입??
			// } else {
			// 	value[0] = deadHost
			// }

			res, _ := json.Marshal(value)
			log.Printf("dead Set Slave list  %s", string(res))

			info := config.GetServerInfo()
			key := fmt.Sprintf("%s/Slaves", info.AppName)
			etcd_SetData(key, string(res))

			etcd_GetDeadKeyDelete()
		}
	}
}

func main() {
	log.Printf("initialization ....")
	resp, client := etcd_GetMasterData()

	key := resp.Node.Key
	log.Printf("key  >>>>>>>>>>>>: %s\n", key)

	etcd_chan := make(chan *etcd.Response)

	go client.Watch(key, 0, false, etcd_chan, nil)
	go receiveEvent(etcd_chan)

	source := make(chan Event)
	output := make(chan Event)
	quit := make(chan bool)

	go watchServer(source, quit, etcd_chan)
	go coalesceMessage(source, output)
	receive(output, quit)
}
