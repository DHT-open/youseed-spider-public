#!/usr/bin/env python
# encoding: utf-8

"""
用于纸上烤鱼磁力程序
爬虫版本1.0
@web程序猿
"""

import socket
import hashlib
import os
import SimpleXMLRPCServer
import time
import datetime
import traceback
import sys
import json
import threading
from hashlib import sha1
from random import randint
from socket import inet_ntoa
from collections import deque
from Queue import Queue
import math
from struct import pack, unpack
from threading import Timer, Thread
from bencode import bencode, bdecode
import binascii
import random
import logging
import redis
import pymysql
from DBUtils.PooledDB import PooledDB
import pika

########爬虫编号#######
SPIDER_ID = 1

########日志配置，同时输出到文件和控制台#######
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

############抓取配置###########
COMMIT_BATCH = 5000             #处理多少条信息后，提交一次
HASH_UPDATE_SIZE_RATE = 1       #每批数据中，提交更新的数据设置为新爬取资源的多少倍，注意：最大不超过HASH_UPDATE_MAX_SIZE
HASH_UPDATE_MAX_SIZE = 300      #每批数据中，最多提交多少条待更新的hash信息
FILELIST_MAX_SIZE = 500         #文件列表中的文件数，最多保留多少条。例如文件列表有1000条，只保留前500个文件名，防止数据过大

MAX_LOCAL_CACHE = 200000        #下载失败的hash和数据库已有的hash缓存大小，每次到达上限后缩减10%

############数据库配置###########
DB_NAME = 'zsky'
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASS = ''
DB_PORT = 3306

############Redis缓存配置###########
R_HOST = '127.0.0.1'            #本机IP
R_PORT = 6379                   #本机端口

R_DB_HASH_ID = 14               #本机已有的short_hash缓存，减少数据库检索，存储格式：Map<short_hash:1>
R_DB_BAD_HASH = 15              #最近发现的无法下载的short_hash缓存，减少数据库检索

HASH_ID_EXPIRES = 3600 * 24 * 1 #已存在的Hash缓存多久
BAD_HASH_EXPIRES = 3600 * 1     #无法下载的short_hash缓存缓存多久

############RabbitMQ消息队列配置###########
MQ_HOST = '127.0.0.1'
MQ_PORT = 5672
MQ_USER = 'youseed'
MQ_PASSWD = 'youseed'
MQ_VIRTUAL_HOSTS = '/'

#用于存储和搜索的数据，各自保存几个副本
MQ_STORE_NUM = 1
MQ_SEARCH_NUM = 0

##入库的交换器
MQ_STORE_EXCHANGE = 'store'
#入库队列前缀
MQ_STORE_HASH_QUEUE_PREFIX = 'store.new'
MQ_STORE_UPDATE_QUEUE_PREFIX = 'store.update'
MQ_STORE_SPIDER_QUEUE_PREFIX = 'store.stat'

##入搜索引擎的交换器
MQ_SEARCH_EXCHANGE = 'search'
#入搜索引擎队列前缀
MQ_SEARCH_HASH_QUEUE_PREFIX = 'search.new'
MQ_SEARCH_UPDATE_QUEUE_PREFIX = 'search.update'

############DHT配置###########
BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("tracker.vanitycore.co", 6969),
    ("inferno.demonoid.pw", 3418),
    ("open.facedatabg.net", 6969),
    ("tracker.mg64.net", 6969),
    ("111.6.78.96", 6969),
    ("90.179.64.91", 1337),
    ("51.15.4.13", 1337),
    ("88.198.231.1", 1337),
    ("151.80.120.112", 2710),
    ("191.96.249.23", 6969),
    ("35.187.36.248", 1337),
    ("123.249.16.65", 2710),
    ("210.244.71.25", 6969),
    ("9.rarbg.me", 2710),
    ('router.utorrent.com',6881),
    ("p4p.arenabg.com", 1337),
    ("ipv4.tracker.harry.lu", 80),
    ('dht.transmission.com',6881),
    ("mgtracker.org", 2710),
    ("tracker.coppersurfer.tk", 6969),
    ("nyaa.tracker.wf", 7777),
    ('tracker.acgnx.se',80),
    ("tracker.coppersurfer.tk", 6969),
    ("tracker.open-internet.nl", 6969),
    ("tracker.skyts.net", 6969),
    ("tracker.piratepublic.com", 1337),
    ("tracker.opentrackr.org", 1337),
    ("allesanddro.de", 1337),
    ("public.popcorn-tracker.org", 6969),
    ("wambo.club", 1337),
    ("tracker4.itzmx.com", 2710),
    ("tracker2.christianbro.pw", 6969),
    ("thetracker.org", 80),
    ("tracker1.wasabii.com.tw", 6969),
    ("tracker.zer0day.to", 1337),
    ("tracker.xku.tv", 6969),
    ("62.138.0.158", 6969),
    ("87.233.192.220", 6969),
    ("78.142.19.42", 1337),
    ("173.254.219.72", 6969),
    ("51.15.76.199", 6969),
    ("91.212.150.191", 3418),
    ("103.224.212.222", 6969),
    ("77.91.229.218", 6969),
    ("5.79.83.193", 6969),
    ("51.15.40.114", 80),
    ("5.196.76.15", 80)
)
TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2
BT_PROTOCOL = "BitTorrent protocol"
BT_MSG_ID = 20
EXT_HANDSHAKE_ID = 0




############GO###########
def get_extension(name):
    return os.path.splitext(name)[1]

def get_category(ext):
    ext = ext + '.'
    cats = {
        u'影视': '.avi.mp4.rmvb.m2ts.wmv.mkv.flv.qmv.rm.mov.vob.asf.3gp.mpg.mpeg.m4v.f4v.',
        u'图像': '.jpg.bmp.jpeg.png.gif.tiff.webp',
        u'文档书籍': '.pdf.isz.chm.txt.epub.bc!.doc.ppt.xls.docx.pptx.xlsx.',
        u'音乐': '.mp3.ape.wav.dts.mdf.flac.',
        u'压缩文件': '.zip.rar.7z.tar.gz.iso.dmg.pkg.',
        u'安装包': '.exe.app.msi.apk.'
    }
    for k, v in cats.iteritems():
        if ext in v:
            return k
    return u'其他'

def get_detail(y):
    if y.get('files'):
        y['files'] = [z for z in y['files'] if not z['path'].startswith('_')]
    else:
        y['files'] = [{'path': y['name'], 'length': y['length']}]
    y['files'].sort(key=lambda z:z['length'], reverse=True)
    bigfname = y['files'][0]['path']
    ext = get_extension(bigfname).lower()
    y['category'] = get_category(ext)
    y['extension'] = ext
    
def random_id():
    hash = sha1()
    hash.update(entropy(20))
    return hash.digest()

def send_packet(the_socket, msg):
    the_socket.send(msg)

def send_message(the_socket, msg):
    msg_len = pack(">I", len(msg))
    send_packet(the_socket, msg_len + msg)

def send_handshake(the_socket, infohash):
    bt_header = chr(len(BT_PROTOCOL)) + BT_PROTOCOL
    ext_bytes = "\x00\x00\x00\x00\x00\x10\x00\x00"
    peer_id = random_id()
    packet = bt_header + ext_bytes + infohash + peer_id

    send_packet(the_socket, packet)

def check_handshake(packet, self_infohash):
    try:
        bt_header_len, packet = ord(packet[:1]), packet[1:]
        if bt_header_len != len(BT_PROTOCOL):
            return False
    except TypeError:
        return False

    bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
    if bt_header != BT_PROTOCOL:
        return False

    packet = packet[8:]
    infohash = packet[:20]
    if infohash != self_infohash:
        return False

    return True

def send_ext_handshake(the_socket):
    msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencode({"m":{"ut_metadata": 1}})
    send_message(the_socket, msg)

def request_metadata(the_socket, ut_metadata, piece):
    """bep_0009"""
    msg = chr(BT_MSG_ID) + chr(ut_metadata) + bencode({"msg_type": 0, "piece": piece})
    send_message(the_socket, msg)

def get_ut_metadata(data):
    ut_metadata = "_metadata"
    index = data.index(ut_metadata)+len(ut_metadata) + 1
    return int(data[index])

def get_metadata_size(data):
    metadata_size = "metadata_size"
    start = data.index(metadata_size) + len(metadata_size) + 1
    data = data[start:]
    return int(data[:data.index("e")])

def recvall(the_socket, timeout=5):
    the_socket.setblocking(0)
    total_data = []
    data = ""
    begin = time.time()

    while True:
        time.sleep(0.05)
        if total_data and time.time()-begin > timeout:
            break
        elif time.time()-begin > timeout*2:
            break
        try:
            data = the_socket.recv(1024)
            if data:
                total_data.append(data)
                begin = time.time()
        except Exception:
            pass
    return "".join(total_data)
    
def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in xrange(length))


def random_id():
    h = sha1()
    h.update(entropy(20))
    return h.digest()


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n

    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        n.append((nid, ip, port))

    return n


def timer(t, f):
    Timer(t, f).start()


def get_neighbor(target, nid, end=10):
    return target[:end]+nid[end:]


class KNode(object):

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTClient(Thread):

    def __init__(self, max_node_qsize):
        Thread.__init__(self)
        self.setDaemon(True)
        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = deque(maxlen=max_node_qsize)

    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except Exception:
            pass

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid, self.nid) if nid else self.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id()
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def re_join_DHT(self):
        if len(self.nodes) == 0:
            self.join_DHT()
        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
#         i = 0
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                pass
                
            try:
                time.sleep(wait)
            except KeyboardInterrupt:
                os._exit(0)
            
#             if i == 5000:
#                 logger.info('[当前NODE队列：+'+ str(len(self.nodes)) +']')
#                 i =0
#             
#             i = i+1
        
    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            if ip == self.bind_ip: continue
            n = KNode(nid, ip, port)
            self.nodes.append(n)


class DHTServer(DHTClient):

    def __init__(self, master, bind_ip, bind_port, max_node_qsize):
        DHTClient.__init__(self, max_node_qsize)

        self.master = master
        self.bind_ip = bind_ip
        self.bind_port = bind_port

        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))

        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)


    def run(self):
        self.re_join_DHT()
        while True:
            try:
                (data, address) = self.ufd.recvfrom(65536)
                msg = bdecode(data)
                self.on_message(msg, address)
            except Exception:
                pass

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if msg["r"].has_key("nodes"):
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

    def on_get_peers_request(self, msg, address):
#         logger.info('on_get_peers_request')
        try:
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = infohash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(infohash, self.nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]

            if infohash[:TOKEN_LENGTH] == token:
                if msg["a"].has_key("implied_port ") and msg["a"]["implied_port "] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                self.master.log_announce(infohash, (address[0], port))
        except Exception:
            pass
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            tid = msg["t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def ok(self, msg, address):
        try:
            tid = msg["t"]
            nid = msg["a"]["id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(nid, self.nid)
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass


class Master(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.queue = Queue(maxsize = 100000)
        self.metadata_queue = Queue(maxsize = 100000)
        
        ########初始化Redis连接
        self.rds_pool_hash_id = redis.ConnectionPool(host=R_HOST,port=R_PORT,db=R_DB_HASH_ID)
        self.rds_pool_bad_hash = redis.ConnectionPool(host=R_HOST,port=R_PORT,db=R_DB_BAD_HASH)

        ########初始化Mysql连接
        self.pool = PooledDB(pymysql,5,host=DB_HOST,user=DB_USER,passwd=DB_PASS,db=DB_NAME,port=DB_PORT,charset="utf8mb4")
        
        ########初始化消息队列
        self.declare_mq()

        ########local cache
        self.hash_update = []
        
        self.downloading = set()#正在下载
        self.visited = set()#近期访问，且已经入库的资源
        self.hash_tosave = []#准备写入redis的已有hash
        
        self.badhash = set()#近期下载失败
        self.badhash_tosave = []#准备写入redis中的下载失败hash
        
        #时间
        self.costs = int(round(time.time() * 1000))
        
        #操作各数据存储组件的耗时合计
        self.mq_costs = self.store_costs = self.redis_costs = 0
        
        #检索资源是否存在时，各存储组件的查询次数
        self.n_cache_hit = self.n_local_bad_hash_hit = self.n_bad_hash_hit = self.n_query = 0
        
        #检索资源的数量
        self.n_stored = self.n_redis_set = self.n_redis_get = self.n_reqs = self.n_new = 0

    #初始化存储消息队列
    def declare_mq(self):
        #连接队列
        credentials = pika.PlainCredentials(MQ_USER, MQ_PASSWD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_HOST, MQ_PORT, MQ_VIRTUAL_HOSTS, credentials, heartbeat=0))
        self.channel = connection.channel()
        
        #存储交换器/队列
        self.channel.exchange_declare(exchange=MQ_STORE_EXCHANGE, exchange_type='topic', durable=True)
        for i in range(MQ_STORE_NUM):
            #新hash
            self.channel.queue_declare(MQ_STORE_HASH_QUEUE_PREFIX + '.' + str(i), False, True, False, False, None)
            self.channel.queue_bind(MQ_STORE_HASH_QUEUE_PREFIX + '.' + str(i), MQ_STORE_EXCHANGE, MQ_STORE_HASH_QUEUE_PREFIX + '.*', None)
            #待更新热度的hash
            self.channel.queue_declare(MQ_STORE_UPDATE_QUEUE_PREFIX + '.' + str(i), False, True, False, False, None)
            self.channel.queue_bind(MQ_STORE_UPDATE_QUEUE_PREFIX + '.' + str(i), MQ_STORE_EXCHANGE, MQ_STORE_UPDATE_QUEUE_PREFIX + '.*', None)
            #爬虫统计信息
            self.channel.queue_declare(MQ_STORE_SPIDER_QUEUE_PREFIX + '.' + str(i), False, True, False, False, None)
            self.channel.queue_bind(MQ_STORE_SPIDER_QUEUE_PREFIX + '.' + str(i), MQ_STORE_EXCHANGE, MQ_STORE_SPIDER_QUEUE_PREFIX + '.*', None)
    
        #搜索引擎交换器/队列
        self.channel.exchange_declare(exchange=MQ_SEARCH_EXCHANGE , exchange_type='topic', durable=True)
        for i in range(MQ_SEARCH_NUM):
            #新hash
            self.channel.queue_declare(MQ_SEARCH_HASH_QUEUE_PREFIX + '.' + str(i), False, True, False, False, None)
            self.channel.queue_bind(MQ_SEARCH_HASH_QUEUE_PREFIX + '.' + str(i), MQ_SEARCH_EXCHANGE , MQ_SEARCH_HASH_QUEUE_PREFIX + '.*', None)
            #待更新热度的hash
            self.channel.queue_declare(MQ_SEARCH_UPDATE_QUEUE_PREFIX + '.' + str(i), False, True, False, False, None)
            self.channel.queue_bind(MQ_SEARCH_UPDATE_QUEUE_PREFIX + '.' + str(i), MQ_SEARCH_EXCHANGE , MQ_SEARCH_UPDATE_QUEUE_PREFIX + '.*', None)
    
    
    def got_torrent(self):
        logger.debug('开始读取种子...')
        binhash, address, data, dtype, start_time = self.metadata_queue.get()
        
        #1.从下载队列中移除该资源
        short_hash = binhash.encode('hex')[0:16] #id
        self.downloading.remove(short_hash)
        
        #2.不处理没数据的资源
        if not data:
            logger.debug('没有种子数据，跳过:'+(binhash.encode('hex')))
            return
        
        #3.解析数据
        try:
            info = self.parse_torrent(data)
            if not info:
                logger.debug('解析种子失败，跳过:'+(binhash.encode('hex')))
                return
        except:
            traceback.print_exc()
            return
        
        info_hash = binhash.encode('hex')
        info['info_hash'] = info_hash
        info['source_ip'] = address[0]

        if info.get('files'):
            files = [z for z in info['files'] if not z['path'].startswith('_')]
            if not files:
                files = info['files']
        else:
            files = [{'path': info['name'], 'length': info['length']}]
           
        files.sort(key=lambda z:z['length'], reverse=True)
        bigfname = files[0]['path']
        info['extension'] = get_extension(bigfname).lower()
        info['category'] = get_category(info['extension'])

        #4.2.1有文件列表时
        if info.get('files'):
            info['file_count'] = len(info['files']) 
            info['filelist'] = info['files'][0:FILELIST_MAX_SIZE] #抛弃多余的文件列表
            del info['files']

        try:
            #4.写入队列
            logger.debug('写入队列：'+ info['info_hash'])
            t1 = int(round(time.time() * 1000))
            
            #4.1写入入库hash队列
            self.channel.basic_publish(
                exchange = MQ_STORE_EXCHANGE, 
                routing_key = MQ_STORE_HASH_QUEUE_PREFIX + '.*',
                body = json.dumps(info),
                properties = pika.BasicProperties(delivery_mode = 2)
            )
            
            logger.debug('写入入库队列：'+ info['info_hash'])
            
            #4.2写入搜索引擎hash队列    
            #4.2.1有文件列表时
            if 'filelist' in info:
                info['filelist_5'] = info['filelist'][0: 5]
                del info['filelist']
            
            self.channel.basic_publish(exchange = MQ_SEARCH_EXCHANGE, 
                routing_key = MQ_SEARCH_HASH_QUEUE_PREFIX + '.*',
                body = json.dumps(info),
                properties = pika.BasicProperties(delivery_mode = 2))
            
            logger.debug('写入搜索引擎队列：'+ info['info_hash'])
            
            t2 = int(round(time.time() * 1000))
            
            #4.3准备写入Redis的已存在Hash的库/记录到最近访问的资源中
            self.hash_tosave.append(short_hash)
            self.visited.add(short_hash)
            
            #4.4计算耗时
            self.mq_costs += (t2 - t1)#记录MQ操作耗时
            
            logger.info('新资源:' + info_hash)
            self.n_new += 1
        
        except:
            logger.info('缓存新资源的消息队列/缓存失败，hash：' + info_hash)
            traceback.print_exc()


    def run(self):
        self.name = threading.currentThread().getName()
        logger.debug('启动种子分析方法，线程名称：' + self.name)
        
        while True:
            while self.metadata_queue.qsize() > 0:
                self.got_torrent()
                
            address, binhash, dtype = self.queue.get()
            
            info_hash = binhash.encode('hex')
            short_hash = info_hash[0:16] #id
            
            logger.debug('处理资源：' + info_hash)
            
            #1.已入库的资源暂时不再处理；正在抓取的新资源不处理
            if short_hash in self.visited:
                logger.debug('资源在近100000次抓取中已有记录，跳过：' + info_hash)
                continue
            
            if short_hash in self.downloading:
                logger.debug('资源正在下载，跳过：' + info_hash)
                continue
            
            #2.抓取计数
            self.n_reqs += 1
            
            try:
                #4.检查资源是否已经入库
                hasHash = False
                t1 = int(round(time.time() * 1000))
                
                #4.1从Redis-已存在的资源中检索
                #虽然查询redis次数较多，但多开爬虫时可以尽可能保持一致性问题；如果不多开爬虫，可以将它放在badhash后，减少redis查询次数
                rds_hash_id = redis.Redis(connection_pool=self.rds_pool_hash_id)
                if rds_hash_id.ping():
                    if rds_hash_id.get(short_hash):
                        hasHash = True
                        self.n_cache_hit += 1
                    self.n_redis_get += 1
                 
                #4.2检索失败缓存中检索self.badhash
                badHash = False
                #4.2.1从本机内存检索
                if not hasHash:
                    if short_hash in self.badhash:
                        badHash = True
                        self.n_local_bad_hash_hit += 1
                        
                #4.2.1从redis检索
                if not hasHash and not badHash:
                    rds_bad_hash = redis.Redis(connection_pool=self.rds_pool_bad_hash)
                    if rds_bad_hash.ping():
                        self.n_redis_get += 1
                        if rds_bad_hash.get(short_hash):
                            badHash = True
                            self.badhash.add(short_hash)
                            self.n_bad_hash_hit += 1
                            
                t2 = int(round(time.time() * 1000))
                self.redis_costs += (t2 - t1)#记录Redis操作耗时
                
                #4.3Redis缓存中都没有记录时，去数据库查询    
                if not hasHash and not badHash:
                    self.n_query += 1
            
                    conn = None
                    curr = None
                    
                    try:
                        t1 = int(round(time.time() * 1000))
                        conn = self.pool.connection()
                        curr = conn.cursor()
                        curr.execute('SELECT id FROM search_hash WHERE info_hash = %s limit 1', (info_hash,))
                        y = curr.fetchone()
                        if y:
                            hasHash = True
                            self.hash_tosave.append(short_hash) #准备写入id缓存

                        t2 = int(round(time.time() * 1000))
                        self.store_costs += (t2 - t1)#记录数据操作耗时
                        
                    finally:
                        if curr:
                            try:
                                curr.close()
                            except:
                                pass
                        if conn:
                            try:
                                conn.close()
                            except:
                                pass     
                 
                #5对资源进行入库操作    
                #5.1如果是已经入库的资源
                if hasHash:
                    logger.debug('资源已经收录：' + short_hash)
                    self.n_stored += 1
                    
                    #5.1.1如果待更新队列中还有空，加入到队列中
                    if len(self.hash_update) < HASH_UPDATE_MAX_SIZE:
                        self.hash_update.append(short_hash)
                    
                    #5.2.2记录到最近访问的资源中
                    self.visited.add(short_hash)
                    
                #5.2如果是未入库资源    
                else:
                    logger.debug('资源未收录，下载种子' + info_hash)
                    
                    #5.2.1记录正在下载的hash
                    self.downloading.add(short_hash)
                    
                    #5.2.2下载种子
                    t = threading.Thread(target=self.download_metadata, args=(address, binhash, self.metadata_queue, badHash))
                    t.setDaemon(True)
                    t.start()
    
                #6.达到一定数目后，记录待更新的hash和统计数据
                if self.n_reqs >= COMMIT_BATCH:
                    
                    #6.0处理一些变量
                    #6.0.0当近期访问记录大于20万时，缩减到19万
                    if len(self.visited) > MAX_LOCAL_CACHE:
                        for i in range(len(self.visited) - int(MAX_LOCAL_CACHE * 0.9)):
                            self.visited.pop()
                    
                    #6.0.0当近期不可用hash大于20万时，缩减到19万
                    if len(self.badhash) > MAX_LOCAL_CACHE:#最大允许存储50000个值
                        for i in range(len(self.badhash) - int(MAX_LOCAL_CACHE * 0.9)):
                            self.badhash.pop()
                    
                    #6.1写入需要更新热度的hash队列
                    self.hash_update = self.hash_update[:self.n_new * HASH_UPDATE_SIZE_RATE]#根据新资源数量，截取需更新数量
                    if len(self.hash_update) > 0 :
                        str_update = json.dumps(self.hash_update)
                        
                        t1 = int(round(time.time() * 1000))
                        #6.1.1存储队列
                        self.channel.basic_publish(exchange = MQ_STORE_EXCHANGE, 
                            routing_key = MQ_STORE_UPDATE_QUEUE_PREFIX + '.*',
                            body = str_update,
                            properties = pika.BasicProperties(delivery_mode = 1))
                        
                        #6.1.2ES队列
                        self.channel.basic_publish(exchange = MQ_SEARCH_EXCHANGE, 
                            routing_key = MQ_SEARCH_UPDATE_QUEUE_PREFIX + '.*',
                            body = str_update,
                            properties = pika.BasicProperties(delivery_mode = 1))
                        
                        t2 = int(round(time.time() * 1000))
                        self.mq_costs += (t2 - t1)#记录MQ操作耗时
                    
                    #6.1.3失败hash写入Redis
                    t1 = int(round(time.time() * 1000))
                    
                    if len(self.badhash_tosave) > 0:
                        rds_bad_hash = redis.Redis(connection_pool=self.rds_pool_bad_hash)
                        if rds_bad_hash.ping():
                            with rds_bad_hash.pipeline(transaction=False) as p:
                                for h in self.badhash_tosave:
                                    self.n_redis_set += 1
                                    p.setex(h, 1, BAD_HASH_EXPIRES)
                                p.execute()
                    
                    #6.1.4已存在hash写入Redis   
                    if len(self.hash_tosave) > 0:
                        rds_hash_id = redis.Redis(connection_pool=self.rds_pool_hash_id)#写入id缓存
                        if rds_hash_id.ping():
                            with rds_hash_id.pipeline(transaction=False) as p:
                                for h in self.hash_tosave:
                                    self.n_redis_set += 1
                                    p.setex(h, 1, HASH_ID_EXPIRES)
                                p.execute()
                    
                    
                    t2 = int(round(time.time() * 1000))
                    self.redis_costs += (t2 - t1)#记录Redis操作耗时
                                       
                    #6.2写入统计队列
                    spider_log = {
                        'spider': SPIDER_ID,
                        'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                        'costs': int(round(time.time() * 1000)) - self.costs,
                        'total': COMMIT_BATCH,
                        
                        'cur_queue': self.queue.qsize(),
                        'cur_downloading': len(self.downloading),
                        
                        'num_new': self.n_new,
                        'num_stored': self.n_stored,
                        'num_update': len(self.hash_update),
                        
                        'cache_hash': len(self.visited),
                        'cache_bad_hash': len(self.badhash),
                        
                        'stat_query': self.n_query,
                        'stat_redis_get': self.n_redis_get,
                        'stat_redis_set': self.n_redis_set,
                        'stat_hash_hit': self.n_cache_hit,
                        'stat_local_bad_hash_hit': self.n_local_bad_hash_hit,
                        'stat_bad_hash_hit': self.n_bad_hash_hit,
                        
                        'costs_store': self.store_costs,
                        'costs_redis': self.redis_costs,
                        'costs_mq': self.mq_costs
                    }
                    
                    self.channel.basic_publish(exchange = MQ_STORE_EXCHANGE, 
                        routing_key = MQ_STORE_SPIDER_QUEUE_PREFIX + '.*',
                        body = json.dumps(spider_log),
                        properties = pika.BasicProperties(delivery_mode = 1))
                    
                    t3 = int(round(time.time() * 1000))
                    
                    #6.3输出统计日志
                    logger.info('[已处理'+ str(COMMIT_BATCH) +'个资源，' + str(self.n_new) + '个新资源，' + str(self.n_stored) + '个已收录，耗时'+ str(spider_log['costs']/1000) +'秒' 
                                ' | 当前待处理Hash队列：' + str(spider_log['cur_queue']) + '，下载中：' + str(spider_log['cur_downloading']) + 
                                ' | 已入库Hash本地缓存：'+str(spider_log['cache_hash']) + '，下载失败Hash本地缓存：'+str(spider_log['cache_bad_hash']) + ']')
                    logger.info('[新Hash：'+str(spider_log['num_new'])+'，提交更新Hash：'+str(spider_log['num_update']) +
                                ' | 数据库检索：'+ str(spider_log['stat_query']) + '，Redis写入：'+ str(spider_log['stat_redis_set']) + '，Redis检索：'+ str(spider_log['stat_redis_get']) +
                                ' | 命中Redis有资源缓存：'  + str(spider_log['stat_hash_hit']) + '，命中Redis无资源缓存：'+ str(spider_log['stat_bad_hash_hit'])  + '，命中本地无资源缓存：'+ str(spider_log['stat_local_bad_hash_hit']) +
                                ' | 数据库操作耗时：'+ str(spider_log['costs_store']) +'毫秒，缓存操作耗时：' + str(spider_log['costs_redis']) + '毫秒，队列操作耗时：'+ str(spider_log['costs_mq']) +'毫秒]')

                    #7.相关变量清0
                    #操作各数据存储组件的耗时合计
                    self.mq_costs = self.store_costs = self.redis_costs = 0
                    
                    #检索资源是否存在时，各存储组件的查询次数
                    self.n_cache_hit = self.n_local_bad_hash_hit = self.n_bad_hash_hit = self.n_query = 0
                    
                    #检索资源的数量
                    self.n_stored = self.n_redis_set = self.n_redis_get = self.n_reqs = self.n_new = 0

                    self.hash_update = []
                    self.badhash_tosave = []
                    self.hash_tosave = []
                    self.costs = int(round(time.time() * 1000))
            except:
                logger.error('读/写Mysql/Redis/RabbitMQ时发生错误')
                traceback.print_exc()

                    
    def decode(self, s):
        if type(s) is list:
            s = ';'.join(s)
        u = s
        for x in (self.encoding, 'utf8', 'gbk', 'big5'):
            try:
                u = s.decode(x)
                return u
            except:
                pass
        return s.decode(self.encoding, 'ignore')

    def decode_utf8(self, d, i):
        if i+'.utf-8' in d:
            return d[i+'.utf-8'].decode('utf8')
        return self.decode(d[i])

    def parse_torrent(self, data):
        info = {}
        self.encoding = 'utf8'
        try:
            torrent = bdecode(data)
            if not torrent.get('name'):
                return None
        except:
            return None

        if torrent.get('encoding'):
            self.encoding = torrent['encoding']
#         if torrent.get('announce'):
#             info['announce'] = self.decode_utf8(torrent, 'announce')
#         if torrent.get('comment'):
#             info['comment'] = self.decode_utf8(torrent, 'comment')[:200]
#         if torrent.get('publisher-url'):
#             info['publisher-url'] = self.decode_utf8(torrent, 'publisher-url')
#         if torrent.get('publisher'):
#             info['publisher'] = self.decode_utf8(torrent, 'publisher')
#         if torrent.get('created by'):
#             info['creator'] = self.decode_utf8(torrent, 'created by')[:15]

        if 'info' in torrent:
            detail = torrent['info'] 
        else:
            detail = torrent
        info['name'] = self.decode_utf8(detail, 'name')
        if 'files' in detail:
            info['files'] = []
            for x in detail['files']:
                if 'path.utf-8' in x:
                    v = {'path': self.decode('/'.join(x['path.utf-8'])), 'length': x['length']}
                else:
                    v = {'path': self.decode('/'.join(x['path'])), 'length': x['length']}
#                 if 'filehash' in x:
#                     v['filehash'] = x['filehash'].encode('hex')
                info['files'].append(v)
            info['length'] = sum([x['length'] for x in info['files']])
        else:
            info['length'] = detail['length']
        info['data_hash'] = hashlib.md5(detail['pieces']).hexdigest()
#         if 'profiles' in detail:
#             info['profiles'] = detail['profiles']
        return info
    threading.stack_size(200*1024)
    socket.setdefaulttimeout(30)

    def log_announce(self, binhash, address=None):
        self.queue.put([address, binhash, 'pt'])


    def download_metadata(self, address, infohash, metadata_queue, badHash, timeout=5):
        logger.debug('开始下载种子资源：'+infohash.encode('hex'));
        metadata = None
        start_time = time.time()
        hash = infohash.encode('hex')
        short_hash = hash[0:16]
        try:
            the_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            the_socket.settimeout(timeout)
            the_socket.connect(address)

            send_handshake(the_socket, infohash)
            packet = the_socket.recv(4096)

            if not check_handshake(packet, infohash):
                logger.debug('下载种子资源握手超时：'+infohash.encode('hex'));
                return

            send_ext_handshake(the_socket)
            packet = the_socket.recv(4096)

            ut_metadata, metadata_size = get_ut_metadata(packet), get_metadata_size(packet)

            metadata = []
            for piece in range(int(math.ceil(metadata_size/(16.0*1024)))):
                request_metadata(the_socket, ut_metadata, piece)
                packet = recvall(the_socket, timeout) #the_socket.recv(1024*17) #
                metadata.append(packet[packet.index("ee")+2:])

            metadata = "".join(metadata)
            
            #如果无效缓存有记录，从无效资源缓存中移除
            if badHash:
                self.badhash.remove(short_hash)
                
                t1 = int(round(time.time() * 1000))
                rds_bad_hash = redis.Redis(connection_pool=self.rds_pool_bad_hash)
                if rds_bad_hash.ping():
                    rds_bad_hash.delete(short_hash)
                t2 = int(round(time.time() * 1000))
                
                self.n_redis_set += 1
                self.redis_costs += (t2 - t1)#记录Redis操作耗时

        except socket.timeout:
            logger.debug('下载种子资源超时：' + hash);
            pass
        except Exception:
            logger.debug('下载种子资源出错：' + hash);
            pass

        finally:
            the_socket.close()
            logger.debug('种子资源处理完毕：' + hash);
            
            
            #下载失败，如果无效缓存没有记录，写入无效资源缓存
            if not metadata and not badHash:
                self.badhash.add(short_hash)
                self.badhash_tosave.append(short_hash)
                
                logger.debug('无资源：' + hash);
            
            metadata_queue.put((infohash, address, metadata, 'pt', start_time))


def announce(info_hash, address):
    binhash = info_hash.decode('hex')
    master.log_announce(binhash, address)
    return 'ok'


def rpc_server():
    rpcserver = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', 8004), logRequests=False)
    rpcserver.register_function(announce, 'announce')
    logger.debug('启动xml rpc server')
    rpcserver.serve_forever()


if __name__ == "__main__":

    logger.info(' __  __                               __              _     __         ')
    logger.info(' \ \/ /___  __  __________  ___  ____/ /  _________  (_)___/ /__  _____')
    logger.info('  \  / __ \/ / / / ___/ _ \/ _ \/ __  /  / ___/ __ \/ / __  / _ \/ ___/')
    logger.info('  / / /_/ / /_/ (__  )  __/  __/ /_/ /  (__  ) /_/ / / /_/ /  __/ /    ')
    logger.info(' /_/\____/\__,_/____/\___/\___/\__,_/  /____/ .___/_/\__,_/\___/_/     ')
    logger.info('                                           /_/                         ')    
    logger.info('开始爬取磁力资源...')

    master = Master()
    master.start()

    rpcthread = threading.Thread(target=rpc_server)
    rpcthread.setDaemon(True)
    rpcthread.start()

    dht = DHTServer(master, "0.0.0.0", 6881, max_node_qsize=10)
    dht.start()
    dht.auto_send_find_node()
