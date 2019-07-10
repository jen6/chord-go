#!/bin/sh

#run three nodes
./node -ip=127.0.0.1 -port=10000 &
sleep 2
./node -ip=127.0.0.1 -port=10001 -successor-ip=127.0.0.1 -successor-port=10000 &
sleep 2
./node -ip=127.0.0.1 -port=10002 -successor-ip=127.0.0.1 -successor-port=10000 &
sleep 2

#insert data on dht
curl -X POST http://127.0.0.1:10000/key/hello -d "data=hello" 
curl -X POST http://127.0.0.1:10001/key/hello -d "data=hello" 
curl -X POST http://127.0.0.1:10002/key/hello -d "data=hello" 

sleep 3
#check all three node can access to data
curl -X GET http://127.0.0.1:10000/key/hello
curl -X GET http://127.0.0.1:10001/key/hello
curl -X GET http://127.0.0.1:10002/key/hello

sleep 3
killall -9 node
