from pymongo import MongoClient
import threading
import redis
import os
import time
import datetime
import traceback
import random
import requests
import json

heads={'Content-Type':'application/x-www-form-urlencoded','User-Agent':'%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','Referer':'http://cnews.qq.com/cnews/android/'}

def idsIntoRedis(pool,idList):
    try:
        rcli=redis.StrictRedis(connection_pool=pool)
        for card in idList:
            qieId=card['chlid']
            if qieId and (rcli.zrank('qieIds_set_right',qieId) == None):
                score=int(rcli.zcard('qieIds_set_right'))+int(rcli.zcard('qieIds_set_left'))
                rcli.zadd('qieIds_set_right',score,qieId)
                print(qieId,'into redis!')
            time.sleep(1)
    except:
        raise

def zlpopzrpush(pool,key1,key2):
    rcli=redis.StrictRedis(connection_pool=pool)
    lua='''
        local Ele = redis.call('ZRANGE',KEYS[1],0,0)[1]
        if not Ele then
            redis.call('RENAME',KEYS[2],KEYS[1])
            Ele = redis.call('ZRANGE',KEYS[1],0,0)[1]
        end
        local Score = redis.call('ZSCORE',KEYS[1],Ele)
        redis.call('ZADD',KEYS[2],Score,Ele)
        redis.call('ZREM',KEYS[1],Ele)
        return Ele
    '''
    ztop=rcli.register_script(lua)
    _id=ztop(keys=[key1,key2])
    _id=_id.decode() if _id else None
    return _id


def parse_channel(channelInfo):
    chlid=channelInfo['chlid']
    chlname=channelInfo['chlname']
    desc=channelInfo['desc']
    icon=channelInfo['icon']
    readCount=channelInfo['readCount']
    followCount=channelInfo['followCount']
    shareCount=channelInfo['shareCount']
    colCount=channelInfo['colCount']
    return {'penguin_id':chlid,'name':chlname,'introduction':desc,'avatar_img':icon,'read_count':readCount,'follower_count':followCount,'fans_count':shareCount,'crawled_at':int(time.time()*1000)}

def parse_clusterInfo(clusterInfo):
    mediaIds=list({'chlid':x['mediaId']} for x in clusterInfo['medialist'] if 'mediaId' in x.keys() and x['mediaId'])
    return mediaIds


def newSMS_into_mongo(news,db):
    for new in news:
        new['state']=0
        db.newSMS.update({'_id':new['id']},new,True)

    