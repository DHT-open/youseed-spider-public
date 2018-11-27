ps -ef|grep dht_spider|grep -v grep|awk '{print $2}'|xargs kill -9
nohup python /opt/spider/dht_spider_zsky.py > /opt/logs/spider.log 2>&1 &
echo 'dht spider for zsky restarted, logs:'
echo 'tail -f /opt/logs/spider.log'