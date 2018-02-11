from pymongo import MongoClient
from bs4 import BeautifulSoup
import threading
import redis
import os
import re
import time
import datetime
import traceback
import random
import requests
import json
import myUtils

user_agents=['%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54660(android)']
uids=['913dfe44789adef6','3ca8f2a9f2104bb0','19ee579b10df50c7']
heads={'Content-Type':'application/x-www-form-urlencoded','Referer':'http://cnews.qq.com/cnews/android/'}
esHeader={'index_name':'qie_logs','type_name':'qie_logs'}

def qieWriter_fetch(chlid):
    #rcli=redis.StrictRedis(connection_pool=pool)
    #heads={'Content-Type':'application/x-www-form-urlencoded','User-Agent':'%E5%A4%A9%E5%A4%A9%E5%BF%AB%E6%8A%A54650(android)','Referer':'http://cnews.qq.com/cnews/android/'}
    data={'android_id':'913dfe44789adef6','apptype':'android','appver':'23_areading_4.6.50','currentTab':'kuaibao','uid':'913dfe44789adef6','needCluster':'yes','chlid':chlid}
    try:
        heads['User-Agent']=random.choice(user_agents)
        uid=random.choice(uids)
        data['android_id']=uid
        data['uid']=uid
        res=requests.post('https://r.cnews.qq.com/getSubItem',headers=heads,data=data)
        #print(res.text)
        res_json=json.loads(res.text)
        channelInfo=res_json['channelInfo']
        clusterInfo=res_json['clusterInfo'] if 'clusterInfo' in res_json.keys() else None
        chlrmation=myUtils.parse_channel(channelInfo)
        mediaIds=myUtils.parse_clusterInfo(clusterInfo) if clusterInfo else []
        esHeader['id']=chlrmation['penguin_id']
        qie_log=[dict(esHeader,**chlrmation)]
        requests.post('http://59.110.52.213/stq/api/v1/pa/shareWrite/add',headers={'Content-Type':'application/json'},data=json.dumps(qie_log))
        print(chlrmation['penguin_id'],'into Elasticsearch!')
        # if len(mediaIds):
        #     myUtils.idsIntoRedis(pool,mediaIds)
        #db.qieWriter.update({'_id':chlrmation['chlid']},chlrmation,True)
        return (chlrmation,mediaIds)
    except:
        traceback.print_exc()
        raise

def getSubNews(chlid,data):
    heads['User-Agent']=random.choice(user_agents)
    uid=random.choice(uids)
    data['android_id']=uid
    data['uid']=uid
    res=requests.post('https://r.cnews.qq.com/getSubNewsIndex',headers=heads,data=data)
    res_json=json.loads(res.text)
    articleIds=res_json['ids'] if 'ids' in res_json else []
    articleSMS=list({'id':x['id'],'comment_count':x['notecount'],'published_at':int(x['timestamp'])*1000,'type':0} for x in articleIds)
    return articleSMS


def getVideoNews(chlid,data):
    heads['User-Agent']=random.choice(user_agents)
    uid=random.choice(uids)
    data['android_id']=uid
    data['uid']=uid
    res=requests.post('https://r.cnews.qq.com/getVideoNewsIndex',headers=heads,data=data)
    res_json=json.loads(res.text)
    videoIds=res_json['ids'] if 'ids' in res_json else []
    videoSMS=list({'id':x['id'],'published_at':int(x['timestamp'])*1000,'type':4} for x in videoIds)
    return videoSMS

def getNews(chlid):
    data={'android_id':'913dfe44789adef6','apptype':'android','appver':'23_areading_4.6.50','currentTab':'kuaibao','uid':'913dfe44789adef6','chlid':chlid}
    articleSMS=getSubNews(chlid,data)
    videoSMS=getVideoNews(chlid,data)
    articleSMS.extend(videoSMS)
    return articleSMS

def qieArticle_fetcher(itemSMS,user_agent):
    heads={'User-Agent':user_agent}
    flag=0
    url="https://kuaibao.qq.com/s/{}/".format(itemSMS['id'])
    itemSMS['up_count']=0
    itemSMS['play_count']=0
    itemSMS['title']=''
    itemSMS['content']=''
    itemSMS['images']=[]
    while 1:
        try:
            res=requests.get(url,headers=heads)
            bs=BeautifulSoup(res.text,'html.parser')
            contentS=bs.select_one('#content')
            tit=contentS.select_one('p.title')
            contbox=contentS.select('div.content-box > p.text')
            imgbox=contentS.select('div.content-box > p[align="center"] > img')
            contlike=bs.select_one('div.contlike > span.contlikenum.J-contlikenum')
            itemSMS['title']=tit.text.strip() if tit else ''
            itemSMS['content']='\n'.join(x.text.strip() for x in contbox if x)
            itemSMS['images']=list(x.get('src') for x in imgbox if x)
            itemSMS['up_count']=int(contlike.text) if contlike and contlike.text.isdecimal() else 0
            return itemSMS
        except:
            if flag>=3:
                return itemSMS
            time.sleep(5)
            flag+=1
            continue


def qieVideo_fetcher(itemSMS,user_agent):
    heads={'User-Agent':user_agent}
    flag=1
    url="https://kuaibao.qq.com/s/{}/".format(itemSMS['id'])
    itemSMS['up_count']=0
    itemSMS['comment_count']=0
    itemSMS['play_count']=0
    itemSMS['title']=''
    itemSMS['content']=''
    itemSMS['images']=[]
    while 1:
        try:
            res=requests.get(url,headers=heads)
            bs=BeautifulSoup(res.text,'html.parser')
            contentS=bs.select_one('#content')
            tit=contentS.select_one('p.title')
            contlike=bs.select_one('div.videoinfo > span.contlikewrap.J-contlike > span.likenum.J-contlikenum')
            playcount=bs.select_one('div.videoinfo > span.playcount')
            itemSMS['title']=tit.text.strip() if tit else ''
            itemSMS['play_count'] = int(playcount.text) if playcount and re.search(r'\d',playcount.text) else 0
            itemSMS['up_count'] = int(contlike.text) if contlike and contlike.text.isdecimal() else 0
            return itemSMS
        except:
            if flag>=3:
                return itemSMS
            time.sleep(5)
            flag+=1
            continue




    


# if __name__=='__main__':
#     pool=redis.ConnectionPool(host='127.0.0.1', port=6379)
#     mcli=mcli = MongoClient('127.0.0.1', 27017)
#     qieWriter_fetch(pool,mcli)