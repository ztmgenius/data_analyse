#!/bin/bash
# vi /etc/crontab and add line :
# 0 6 * * * root source /etc/profile && source /root/.bash_profile && /YourHomePath/batch.sh 2>&1
#home_path=$(pwd)
home_path=/home/test
log_file=$home_path/log/all_log.$(date +%F%n)
echo $(date +%F%n%T) >$log_file 2>&1
cd $home_path

PYTHON=/usr/local/bin/python3
pyfile=("qyweixin_api.py -s 1")
for (( i = 0 ; i < ${#pyfile[@]} ; i++ ))
do
  file=${pyfile[$i]}
  $PYTHON $home_path/$file >>$log_file 2>&1
  if [ $? -eq 0 ];then
    echo $file" is successful" >>$log_file 2>&1
  else
    echo "ERROR:"$file" is aborting" >>$log_file 2>&1
  fi
done

echo $(date +%F%n%T) >>$log_file 2>&1