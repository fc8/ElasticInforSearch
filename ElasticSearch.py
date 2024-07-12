import json
import requests

class ElasticSearch:
    def __init__(self, ip):
        self.ip_back = ip
        self.ip = 'http://' + ip + ':9200'
        self.header = {
            "Host": ip + ":9200",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Content-Type": "application/json"
        }
        self.proxy = {'http':'127.0.0.1:10809'}
    def count(self, keyword):
        url =  self.ip + '/_count'
        data = '{"query":{"query_string":{"query":"'+keyword+'"}}}'
        # data = json.dumps(data)
        # print(resp.text)
        try:
            resp = self.doPost(url, self.header, data)
            if resp.status_code != 200:
                return -1
            resp_json = json.loads(resp.text)
        except Exception as e:
            return -1
        count = resp_json['count']
        return count

    def doPost(self, url, header, data):
        try:
            resp = requests.post(url, headers=header, data=data, proxies=self.proxy, timeout=5)
        except Exception as e:
            return -1
        return resp

    def doGet(self, url, header):
        resp = requests.get(url, headers=header, proxies=self.proxy)
        return resp

    def getDetail(self, index, type, id):
        url = self.ip + '/' + index + '/' + type + '/' + id
        resp = self.doGet(url, self.header)
        resp_json = json.loads(resp.text)
        print(resp_json)

    def searchDetail(self, keyword):
        # url = self.ip + '/*/_search'
        url = self.ip + '/_search'
        # data = '{"query":{"query_string":{"query":"'+ keyword +'"}},"size":10,"from":0,"sort":[]}'
        data = '{"query": {"query_string": {"query": "'+ keyword +'"}}}'
        try:
            resp = self.doPost(url, self.header, data)
            if resp.status_code != 200:
                return -1, -1
            resp_json = json.loads(resp.text)
        except Exception as e:
            return -1, -1
        try:
            total = resp_json['hits']['total']['value'] #一共多少个，查询的size设置的100，小于100就是全部的数量
        except Exception as e:
            total = resp_json['hits']['total']
        #统计这个数量是这次查询出来多少个 => hits.hits
        hits = resp_json['hits']['hits']
        count = len(hits)
        # hits.hits[0]._source
        return total, resp_json

    #获取磁盘大小
    def getStat(self):
        url = self.ip + '/_cluster/stats'
        try:
            resp = self.doGet(url, self.header)
            resp_json = json.loads(resp.text)
            size_in_bytes = resp_json['indices']['store']['size_in_bytes']
            size_in_bytes = int(size_in_bytes) / 1000000
            return size_in_bytes
        except Exception as e:
            return 0
