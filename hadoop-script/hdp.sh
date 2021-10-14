
#!/bin/bash
if [ $# -lt 1 ]
then
        echo "No Args Input..."
        exit;
fi
case $1 in
"start")
        echo "=================== 启动 hadoop 集群 ===================="

        echo "------------------ 启动hdfs ---------------------"
        ssh had01 "/opt/module/hadoop-2.7.2/sbin/start-dfs.sh"
        echo "------------------ 启动yarn ---------------------"
        ssh had03 "/opt/module/hadoop-2.7.2/sbin/start-yarn.sh"
        echo "------------------ 启动historyserver ---------------------"
        ssh had02 "/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh start historyserver"
;;
"stop")

        echo "=================== 关闭 hadoop 集群 ===================="

        echo "------------------ 关闭historyserver ---------------------"
        ssh had02 "/opt/module/hadoop-2.7.2/bin/mapred --daemon stop  historyserver"
        echo "------------------ 关闭yarn ---------------------"
        ssh had03 "/opt/module/hadoop-2.7.2/sbin/stop-yarn.sh"
        echo "------------------ 关闭hdfs ---------------------"
        ssh had02 "/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh stop historyserver"
;;
*)
        echo "Input Args Error..."
;;
esac
