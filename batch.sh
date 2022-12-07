#!/bin/bash
# vi /etc/crontab and add line :
# 0 6 * * * root source /etc/profile && source /root/.bash_profile && /YourHomePath/batch.sh 2>&1
# cat /dev/null >nohup.out
#home_path=$(pwd)
home_path=/home/test
png_path=$home_path/png
log_file=$home_path/log/all_log.$(date +%F%n)
echo $(date +%F%n%T) >>$log_file 2>&1
echo "上次文件总数： "$(ls -l $png_path| wc -l) >>$log_file 2>&1
echo "上次月线文件数:"$(ls -l $png_path/*_month_*| wc -l) >>$log_file 2>&1
echo "上次周线文件数:"$(ls -l $png_path/*_week_*| wc -l) >>$log_file 2>&1
cd $home_path
rm -f $png_path/*.png

PYTHON=/usr/local/bin/python3
pyfile=("apririo_rule.py" "fraud_group.py" "fraud_detection.py" "rfm.py" "shop_sales.py" "user_retention_rate.py" "beike_spider.py" "shop_sales_stat.py 1" "shop_visit_hour.py 1" "brand_value.py" "audit_stat.py" "qyweixin_api.py -s 4" "fraud_predict.py -t 2" "doris_job.py")
pyfile=("apririo_rule.py" "fraud_group.py" "fraud_detection.py" "rfm.py" "shop_sales.py" "user_retention_rate.py" "beike_spider.py" "shop_sales_stat.py 1" "shop_visit_hour.py 1" "brand_value.py" "audit_stat.py" "doris_import.py -d wx_work -p 5" "doris_import.py -p 5" "doris_job.py" "qw_user_tag.py -m3" "qw_user_tag.py -m2" "qyweixin_api.py -s 4")
pyfile=("apririo_rule.py" "fraud_group.py" "fraud_detection.py" "rfm.py" "shop_sales.py" "user_retention_rate.py" "beike_spider.py" "shop_sales_stat.py 1" "shop_visit_hour.py 1" "brand_value.py" "audit_stat.py" "doris_import.py -d umall_dwa -p 8" "doris_import.py -d wx_work -p 8" "doris_import.py -p 8" "shop_join.py" "qyweixin_api.py -s 4")
for (( i = 0 ; i < ${#pyfile[@]} ; i++ ))
do
  file=${pyfile[$i]}
  echo $file
  $PYTHON -u $home_path/$file >>$log_file 2>&1
  if [ $? -eq 0 ];then
    echo $file" is successful" >>$log_file 2>&1
  else
    echo "ERROR:"$file" is aborting" >>$log_file 2>&1
  fi
done

echo "本次月线文件数:"$(ls -l $png_path/*_month_*| wc -l) >>$log_file 2>&1
echo "本次周线文件数:"$(ls -l $png_path/*_week_*| wc -l) >>$log_file 2>&1
echo "本次文件总数： "$(ls -l $png_path| wc -l) >>$log_file 2>&1
echo $(date +%F%n%T) >>$log_file 2>&1
find $home_path/log/ -mtime +30  -exec rm -rf {} \;
