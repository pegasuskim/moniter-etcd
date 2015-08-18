
"etcd install path"/bin

// etcd 실행 
./etcd -data-dir instance -name redisInfo


"etcd install path"/bin/dbmaster.txt 
"host:port"

"etcd install path"/bin/dbslaves.txt 
["host:port","host:port","host:port"]

// 데이터 Set
curl -L "host:port"/v2/keys/KEY/Master -XPUT --data-urlencode value@dbmaster.txt

curl -L "host:port"/v2/keys/KEY/Slaves -XPUT --data-urlencode value@dbslaves.txt

curl -L "host:port"/v2/keys/SP/Dead -XPUT

// 데이터 Get
curl -L "host:port"/v2/keys/KEY/Master
curl -L "host:port"/v2/keys/KEY/Slaves
curl -L "host:port"/v2/keys/KEY/Dead
