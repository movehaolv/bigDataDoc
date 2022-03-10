cur_dir=`pwd`
for i in lh@node01 lh@node02 lh@node03
do
echo "============================$i======================="
ssh $i "cd $cur_dir; $1"
done


cur_dir=`pwd`
for i in sas@29.23.103.1 sas@29.23.103.3 sas@29.23.103.5  sas@29.23.103.6
do
echo "============================$i======================="
ssh $i "cd $cur_dir; $1"
done
