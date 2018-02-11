from pymongo import MongoClient
import threading
import redis
import os
import time
import traceback
from elasticsearch import Elasticsearch
import socket
import random
import json
import requests
import qieContext_fetcher
import myUtils


def all_fetcher_thread(pool, db1,db2,user_agents):
    for i in range(3):
        t1=threading.Thread(target=theForeman,args=(pool,db1,db2,user_agents))
        t1.start()

   



def theForeman(pool,db1,db2,user_agents):
    rcli=redis.StrictRedis(connection_pool=pool)
    chlid=None
    while 1:
        try:
            db=db1 if db1.client.is_primary else db2
            chlid=myUtils.zlpopzrpush(pool,'qieIds_set_right','qieIds_set_left') if not chlid else chlid
            writer=qieContext_fetcher.qieWriter_fetch(chlid)
            chlrmation=writer[0]
            mediaIds=writer[1]
            news=qieContext_fetcher.getNews(chlid)
            newsToMongo=threading.Thread(target=myUtils.newSMS_into_mongo,args=(news,db))
            newsToMongo.start()
            db.qieWriter.update({'_id':chlrmation['penguin_id']+'_'+time.strftime("%Y-%m-%d",time.localtime())},chlrmation,True)
            if len(mediaIds):
                myUtils.idsIntoRedis(pool,mediaIds)
            del chlrmation['crawled_at']
            for new in news:
                del new['state']
                if new['type']==0:
                    itemSMS=qieContext_fetcher.qieArticle_fetcher(new,random.choice(user_agents))
                else:
                    itemSMS=qieContext_fetcher.qieVideo_fetcher(new,random.choice(user_agents))
                itemSMS['index_name']='qie_articles_and_users'
                itemSMS['type_name']='qie_articles_and_users'
                itemSMS['crawled_at']=int(time.time()*1000)
                itemSMS['resource_id']=itemSMS['id']
                item=[dict(chlrmation,**itemSMS)]

                #rcli.lpush('qie_ES_list',item)
                status=requests.post('http://59.110.52.213/stq/api/v1/pa/shareWrite/add',headers={'Content-Type':'application/json'},data=json.dumps(item))
                print(itemSMS['id'],'into Elasticsearch!')
                #print(status)
                new['state']=1
                #print(new)
                db.newSMS.update({'_id':new['resource_id']},new,True)
                time.sleep(3)
            chlid=None
            #break
        except:
            traceback.print_exc()
            time.sleep(20)
            continue
            
            
                
            

def do_main():
    with open(os.path.abspath('.') + '/Redis_Mongo_Es' + '/redis_mongo_es.txt', 'r', encoding='utf-8') as f:
        line=f.read()
    with open(os.path.abspath('.') + '/UserAgent' + '/user_agent.txt', 'r', encoding='utf-8') as u:
        user_agents=u.read().split('\n')[0:-1]
    _dict = eval(line)
    red_dict = _dict['red']
    mon_dict = _dict['mon1']
    mon_dict2 = _dict['mon2']
    es_dict = _dict['es']
    red_host = red_dict['host']
    red_port = int(red_dict['port'])
    red_pwd = red_dict['password']
    mon_host = mon_dict['host']
    mon_port = str(mon_dict['port'])
    mon_user = mon_dict['user']
    mon_pwd = mon_dict['password']
    mon_dn = mon_dict['db_name']
    mon2_host = mon_dict2['host']
    mon2_port = str(mon_dict2['port'])
    mon2_user = mon_dict2['user']
    mon2_pwd = mon_dict2['password']
    mon2_dn = mon_dict2['db_name']
    es_url=es_dict['url']
    es_port=es_dict['port']
    es_name=es_dict['name']
    es_pwd=es_dict['password']
    mon_url='mongodb://' + mon_user + ':' + mon_pwd + '@' + mon_host + ':' + mon_port +'/'+ mon_dn+'?maxPoolSize=8'
    mon_url2 = 'mongodb://' + mon2_user + ':' + mon2_pwd + '@' + mon2_host + ':' + mon2_port + '/' + mon2_dn+'?maxPoolSize=8'
    rpool = redis.ConnectionPool(host=red_host, port=red_port,password=red_pwd)
    #rpool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    es = Elasticsearch([es_url], http_auth=(es_name, es_pwd), port=es_port)
    #es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    # mcli = MongoClient('127.0.0.1', 27017)
    # mcli2 = MongoClient('127.0.0.1', 27017)
    mcli = MongoClient(mon_url)
    mcli2 = MongoClient(mon_url2)
    db1 = mcli.get_database('penguins')
    db2 = mcli2.get_database('penguins')
    working_thread = threading.Thread(target=all_fetcher_thread, args=(rpool, db1,db2,user_agents))
    working_thread.start()
    print('Penguins crawlers start to work!')
    working_thread.join()
if __name__=='__main__':
    try:
        do_main()
    except:
        traceback.print_exc()
