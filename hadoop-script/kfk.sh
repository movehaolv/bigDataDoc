#!/bin/bash

case $1 in 
"start"){
	for i in lh@node01 lh@node02 lh@node03
	do
		echo "============== $i ================"
	ssh $i 'source /etc/profile && /opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties'
	done
	
	
};;

"stop"){
	for i in lh@node01 lh@node02 lh@node03
	do
		echo "============== $i ================"
	ssh $i 'source /etc/profile && /opt/module/kafka/bin/kafka-server-stop.sh stop'
	done
	
	
};;

esac
