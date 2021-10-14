#!/bin/bash

case $1 in 
"start"){
	for i in lh@node01 lh@node02 lh@node03
	do
		echo "============== $i ================"
		ssh $i 'source /etc/profile && /opt/module/zookeeper-3.4.10/bin/zkServer.sh start'
	done
	
	
};;

"stop"){
	for i in lh@node01 lh@node02 lh@node03
	do
		echo "============== $i ================"
		ssh $i 'source /etc/profile && /opt/module/zookeeper-3.4.10/bin/zkServer.sh stop'
	done
	
	
};;

esac
