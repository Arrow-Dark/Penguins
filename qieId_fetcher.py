from pymongo import MongoClient
import threading
import redis
import os
import time
import datetime
import traceback
from elasticsearch import Elasticsearch
import random
import requests
import json
import myUtils

# def getInterest(pool):
#     rcli = redis.StrictRedis(connection_pool=pool)
#     heads={'Content-Type':'application/x-www-form-urlencoded','User-Agent':'%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','Referer':'http://cnews.qq.com/cnews/android/'}
#     data={'currentTab':'kuaibao','chlid':'daily_timeline','appver':'23_areading_4.6.50','apptype':'android','uid':'913dfe44789adef6','android_id':'913dfe44789adef6'}
#     while 1:
#         res=requests.post('https://r.cnews.qq.com/getSubNewsInterest',headers=heads,data=data)
#         #print(res.text)
#         res_json=json.loads(res.text)
#         newslist=res_json['newslist']
#         if len(newslist):
#             del newslist[0]
#             for new in newslist:
#                 qieId=new['chlid']
#                 rcli.zadd('qieIds_zset',rcli.zcard('qieIds_zset'),qieId)
#                 print(qieId,'into redis!')
#         time.sleep(8)

def getMedia(pool):
    #rcli = redis.StrictRedis(connection_pool=pool)
    heads={'Content-Type':'application/x-www-form-urlencoded','User-Agent':'%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','Referer':'http://cnews.qq.com/cnews/android/'}
    data={'android_id':'913dfe44789adef6','apptype':'android','appver':'23_areading_4.6.50','currentTab':'kuaibao','uid':'913dfe44789adef6'}
    while 1:
        try:
            res=requests.post('https://r.cnews.qq.com/getForcusMedia',headers=heads,data=data)
            #print(res.text)
            res_json=json.loads(res.text)
            cardList=res_json['cardList']
            if len(cardList):
                myUtils.idsIntoRedis(pool,cardList)
            else:
                time.sleep(8)
        except:
            traceback.print_exc()
            time.sleep(8)
            continue
    



if __name__=='__main__':
    pool=redis.ConnectionPool(host='101.201.227.186',port=6005,password='Abc123098')
    #getInterest(pool)
    getMedia(pool)
