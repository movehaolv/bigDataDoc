#!/bin/bash
for i in hadoop@gyh102 hadoop@gyh103 hadoop@gyh104
do
    echo "============== $i ================"
    ssh $i '/opt/module/jdk1.8.0_144/bin/jps'
done



