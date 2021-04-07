#!/bin/bash

case $1 in 
"start"){
	for i in lh@had01 lh@had02 lh@had03
	do
		echo "============== $i ================"
	ssh $i 'source /etc/profile && /opt/module/kafka_2.11/bin/kafka-server-start.sh -daemon /opt/module/kafka_2.11/config/server.properties'
	done
	
	
};;

"stop"){
	for i in lh@had01 lh@had02 lh@had03
	do
		echo "============== $i ================"
	ssh $i 'source /etc/profile && /opt/module/kafka_2.11/bin/kafka-server-stop.sh stop'
	done
	
	
};;

esac
