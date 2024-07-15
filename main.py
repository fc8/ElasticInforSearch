import os
import time
from queue import Queue
from ElasticSearch import ElasticSearch
import threading
from concurrent.futures import ThreadPoolExecutor

write_Count_Lock = threading.Lock()  # 统计数据的锁
write_detail_Lock = threading.Lock()  # 详细信息的锁
pool = ThreadPoolExecutor(max_workers=20)  # 线程池
info_queue = Queue()

# 统计关键字出现的次数
def count():
    with open('ip', 'r') as ip, open('keywords', 'r', encoding='utf-8') as keyword:
        ip_line = ip.readlines()  # 读取ip列表
        keyword_line = keyword.readlines()  # 读取关键字列表
        for l in ip_line:
            ip = l.strip()
            ela = ElasticSearch(ip)
            if ela.getStat() < 1000:  # 判断数据库大小
                continue
            for k in keyword_line:
                keyword = k.strip()
                # getCount(ela, keyword)
                threading.Thread(target=getCount, args=(ela, keyword)).start()

#  询关键词出现的数量
def getCount(ela, keyword):
    resp = ela.count(keyword.strip())
    if resp > 0:
        with write_Count_Lock:
            info_queue.put([ela.ip_back, keyword])
            with open('hasData', 'a', encoding='utf-8') as d:
                d.write(ela.ip_back + " " + keyword + " " + str(resp) + "\n")
                print(ela.ip_back, keyword)

#获取前n条带有关键字的信息
def getDetail(ip, keyword):
    ela = ElasticSearch(ip)
    total, resp = ela.searchDetail(keyword)
    if total > 0:
        with write_detail_Lock:
            with open('result\\'+ip+"\\"+keyword, 'a', encoding='utf-8') as result:
                result.write("查询数量==>"+str(total)+"\n")
                for d in resp['hits']['hits']:
                    print(ip + "==[" + keyword + "]=>" + str(d['_source']))
                    result.write(ip + "===[" + keyword + "]===>" + str(d['_source']) + "\n")
def Detail():
    with open('hasData', 'r', encoding='utf-8') as r:
        lines = r.readlines()
        for l in lines:
            temp = l.strip().split(" ")
            if not os.path.exists('result\\' + temp[0]):
                os.makedirs('result\\' + temp[0])
            pool.submit(getDetail, *(temp[0], temp[1]))
            time.sleep(0.5)

def Detail_Queue():
    while True:
        if info_queue.empty():
            continue
        line = info_queue.get()
        ip, keyword = line[0], line[1]
        if not os.path.exists('result\\' + ip):
            os.makedirs('result\\' + ip)
        threading.Thread(target=getDetail, args=(ip, keyword)).start()


if __name__ == '__main__':
    print("""
   ________
  < Search >
   --------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/
                ||----w||
                ||     ||
                """)
    while True:
        ipt = input("进行什么操作：\n 1.查数量，同步查详情\n 2.查数量\n 3.查详情")
        if ipt == '1':
            threading.Thread(target=Detail_Queue).start()
            count()
        elif ipt == '2':
            count()
        elif ipt == '3':
            Detail()