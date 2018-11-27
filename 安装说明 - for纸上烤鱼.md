# Youseed磁力爬虫安装说明（for 纸上烤鱼） #

> 名为`dht_spider_zsky.py`的这个爬虫是为“纸上烤鱼磁力程序”改版的，安装后可以直接使用。
> 
> 纸上烤鱼的Github：https://github.com/wenguonideshou/zsky


**注意**：以下均以centos7为例，部分软件可能已经安装。

# 1.安装python及依赖环境 #

    yum install -y python-devel
    yum -y install python-pip

	pip install bencode.py
	pip install PyMySQL
	pip install redis
	pip install DBUtils
	pip install pika

# 2.安装redis #

	yum -y install redis

# 3.安装rabbitMQ #

**注意**：rabbitMQ的安装配置稍微麻烦一点，需要一定的耐心。如果出现了问题，建议搜索一下“rabbitMQ安装教程”。

	wget https://github.com/rabbitmq/erlang-rpm/releases/download/v21.1.3/erlang-21.1.3-1.el7.centos.x86_64.rpm
	rpm -ivh erlang-21.1.3-1.el7.centos.x86_64.rpm

	wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.7.9/rabbitmq-server-3.7.9-1.el7.noarch.rpm
	rpm -ivh rabbitmq-server-3.7.9-1.el7.noarch.rpm

	rabbitmq-plugins enable rabbitmq_management
	
	systemctl start rabbitmq-server
	systemctl enable rabbitmq-server

	rabbitmqctl add_user  youseed youseed
	rabbitmqctl set_user_tags youseed administrator
	rabbitmqctl set_permissions -p / youseed '.*' '.*' '.*'

# 4.数据库... #
	直接使用“纸上烤鱼”数据库。

# 5.安装、配置爬虫 #

1.下载`dht_spider_zsky.py`到本地；

2.修改爬虫配置，使用文本编辑器打开`dht_spider_zsky.py`，修改如下配置：

    DB_NAME = 'zsky'
    DB_HOST = '127.0.0.1'
    DB_USER = 'root'
    DB_PASS = '' <---------------zsky数据库密码
    DB_PORT = 3306

**注意**：爬虫的可配置项较多，在正常运行后，可以根据需要自行调整
    

3.启动爬虫（需要确保纸上烤鱼自带的爬虫已经停止）

	python dht_spider_zsky.py

或者后台启动

	nohup python dht_spider_zsky.py > spider.log 2>&1 &

# 5.检查是否正常工作 #

这时，爬虫已经在工作了。但是保险起见，依次执行如下检查：

1.检查爬虫是否正常工作：爬虫在抓取到新资源时，会输出类似如下的日志：

    新资源:2345f88fdc4a726bdbc965c8c19ba783c5c2c6xx

**注意**：首次运行爬虫时，需要等待一段时间才能抓取到资源，这是由DHT网络的工作原理决定的。

2.使用浏览器打开如下地址（可能需要在防火墙打开15672端口）：

    http://<服务器ip>:15672

使用账户`youseed`，密码`youseed`登录后，在菜单`Queues`下的表格中，查看到行`store.new.0`的`Ready`字段的值不断增长。它代表了抓取到的资源条目。


# 6.进一步加强服务器安全 #

1. 在安装时，rabbitMQ的账户和密码预设为`youseed/youseed`，如果程序完全运行在防火墙内，不需要修改；如果爬虫和入库程序在不同机器上，则建议修改这个密码。
2. 如果不需要机器外访问，请禁止外部访问防火墙端口5672和15672。