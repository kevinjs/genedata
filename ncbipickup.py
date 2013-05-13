'''
Created on May 11, 2013

@author: jingshao@cnic.cn
'''

import sys, string, getopt
import gzip, math, hashlib
import json, datetime

import random, time
import pymongo, gridfs

def readInstance(range_s=0, target='product'):
    global conn_mongo
    global mongo_db
    
    print 'Read instances ...'
    
    db_ncbi_data = conn_mongo[mongo_db]
    col_query = db_ncbi_data['query']
    col_meta = db_ncbi_data['meta']
    col_errs = db_ncbi_data['errs']
    
    item_id = -1
    c = 0
    query = {}
    
    if range_s != None:
        query = {'_id':{'$gte':range_s}}
    else:
        query = None
    
    try:
#    for item in col_meta.find({'_id':{'$gte':range_s, '$lt':(range_s + len)}}):
        for item in col_meta.find(query, timeout=False).sort("_id", pymongo.ASCENDING):
            item_id = item['_id']
            target_array = []
            for key in item['FEATURES']:
                if key['key_table'].has_key(target):
                    if target_array.count(key['key_table'][target]) == 0:
                        target_array.append(key['key_table'][target])
            col_query.update({'_id':item_id}, {"$set":{'PRODUCT':target_array}})
            
            c = c + 1
            if c % 10000 == 0:
                print str(int(item_id))
    except:
        print 'Error @ _id: ' + str(item_id)
        errinfo = sys.exc_info()
        print errinfo
        timeStr = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        col_errs.insert({'item_id':item_id, 'ctime':timeStr, 'event': 'pickup_product'})
    finally:
        conn_mongo.close()
        

if __name__ == '__main__':
    start_ms = datetime.datetime.now()
    
    mongo_host = '192.168.40.81'
    mongo_port = 29025
    mongo_db = 'ncbi_data'
    
    conn_mongo = pymongo.Connection(mongo_host, mongo_port)
    
    readInstance(range_s=62922990)
    
    end_ms = datetime.datetime.now()
    print 'cost: ' + str((end_ms - start_ms).seconds) + ' s'
    pass
