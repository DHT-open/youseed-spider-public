

此程序使用python语言，在最流行的sim_dht_spider.py基础之上改写，具有更低的资源占用和更高的爬取性能。

# 功能 #

抓取磁力元数据，并保存到消息队列。

**注意：**此爬虫程序主要负责抓取数据，需要配合“youseed-spider-saver-public”程序保存至数据库。

# 程序特点 #

1. 多级缓存：使用python内存、redis缓存存储一段时间内的有效/无效资源信息，大幅减少了数据库检索次数，提高了抓取效率；
2. 异步存储：采用消息队列解耦抓取和入库环节，支持一个爬虫抓取数据，异步保存到多个数据库；
3. 实时索引：对实时索引的搜索引擎提供了支持，可以将抓取到的数据即时更新到搜索引擎；
4. 逻辑优化：大幅提高了已失败资源的重新抓取频率（实验性改进）；可以自行调整新资源/旧资源入库比例


# 硬件要求 #

- 空闲内存1G以上：爬虫的爬取速度越快，内存占用越高，所以内存越大越好，要避免redis进入swap，造成性能下降
- CPU 1G以上：爬取速度越快，CPU占用越高。从经验上来说，需要Atom以上级别的CPU
- 默认需开放8004和6881端口：TCP和UDP协议


# 软件要求 #

需要安装以下软件：

- python
- redis
- rabbitMQ
- Mongodb/Mysql（根据需要选择）

# 安装（以centos7为例） #

## 安装python及依赖环境 ##

    yum install -y python-devel
    yum -y install python-pip

	pip install bencode.py
	pip install PyMySQL
	pip install redis
	pip install DBUtils
	pip install pika

## 安装redis ##

	yum -y install redis

## 安装rabbitMQ ##

rabbitMQ的安装配置稍微麻烦一点，需要一定的耐心。如果出现了问题，建议搜索一下“rabbitMQ安装教程”。

centos7可以用如下命令安装

	wget https://github.com/rabbitmq/erlang-rpm/releases/download/v21.1.3/erlang-21.1.3-1.el7.centos.x86_64.rpm
	rpm -ivh erlang-21.1.3-1.el7.centos.x86_64.rpm

	wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.7.9/rabbitmq-server-3.7.9-1.el7.noarch.rpm
	rpm -ivh rabbitmq-server-3.7.9-1.el7.noarch.rpm

	rabbitmq-plugins enable rabbitmq_management
	rabbitmq-server

	rabbitmqctl add_user  youseed youseed
	rabbitmqctl set_user_tags root administrator
	rabbitmqctl set_permissions -p / youseed '.*' '.*' '.*'

## 数据库... ##
	直接使用“纸上烤鱼zsky”数据库。

## 安装、配置爬虫 ##

1. 下载程序至本地磁盘

	- 下载`dht_spider_zsky.py`到服务器硬盘。

2. 修改爬虫配置，使用文本编辑器打开“dht_spider_zsky.py”或“dht_spider.py”，修改如下配置：

    DB_NAME = 'zsky'
    DB_HOST = '127.0.0.1'
    DB_USER = 'root'
    DB_PASS = '' <---------------数据库密码
    DB_PORT = 3306

	**注意：**爬虫的可配置项较多，可以根据需要自行修改
    

3. 启动爬虫

	python dht_spider_zsky.py

	或者后台启动

	nohup python dht_spider_zsky.py > spider.log 2>&1 &