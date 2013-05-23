'''
Created on May 11, 2013

@author: root
'''

import sys, string, getopt
import gzip, math, hashlib
import json, datetime

import random, time
import pymongo, gridfs
import re

import traceback

def appendFile(content, filename):
    if len(content) != 0:
        output = open(filename, 'a')
        output.write(content)
        output.close()

#def location_proc2(location):
#    global nums_p
#    
#    location = location.replace('complement(', '')
#    location = location.replace('join(', '')
#    location = location.replace(')', '')
#    location = location.replace('<', '')
#    location = location.replace('>', '')
#    
#    if location.find(',') == -1:
#        se = location.split('..')
#    
#        if len(se) == 2 and se[0].isdigit() and se[1].isdigit():
#            return [int(se[0]), int(se[1])]
#        else:
#            return None
#    else:
#        #join
#        return [0, -1]
    
def location_proc(location):
    
    st = []
    ed = []
    
    location = location.replace('complement(', '')
    location = location.replace('join(', '')
    location = location.replace('order(', '')
    location = location.replace('(', '')
    location = location.replace(')', '')
    location = location.replace('<', '')
    location = location.replace('>', '')
    
    tmps = location.split(',')
    
    for tmp in tmps:
        #reference
        if tmp.find(':') == -1:
            if tmp.find('..') == -1:
                st.append(int(tmp))
                ed.append(int(tmp))
            else:
                st.append(int(tmp.split('..')[0]))
                ed.append(int(tmp.split('..')[1]))
                
    if len(st) == len(ed):
        return [st, ed]
    else:
        return None
    
def check_exist(col_features, meta_id, feature_key, feature_value):
    query = {}
    query['meta_id'] = meta_id
    query['feature_key'] = feature_key
    query['feature_value'] = feature_value

    ins = col_features.find(query).sort("_id", pymongo.ASCENDING)
    
    if ins == None or ins.count() == 0:
        return None
    else:
        return ins[0]['_id']


def readInstance(range_s=0, featureKey='gene', subFeatureKey='gene'):
    global conn_mongo
    global mongo_db
    global logfile
    
    print 'Read instances ...'
    
    db_ncbi_data = conn_mongo[mongo_db]
    col_query = db_ncbi_data['query']
    col_features = db_ncbi_data['features']
    
    col_idtable = db_ncbi_data['idtable']
    col_meta = db_ncbi_data['meta']
    col_errs = db_ncbi_data['errs']
    
    item_id = -1
    query = {}
    kkk = ''
    
    if range_s != None:
        query = {'_id':{'$gte':range_s}}
    else:
        query = None
    
    try:
        c = 0
        insert_array = []
        
        for item_query in col_query.find(query, timeout=False).sort("_id", pymongo.ASCENDING):
            if len(item_query['GENE']) != 0:
                item_id = item_query['_id']
                item = col_meta.find_one({'_id':item_query['_id']})

                for feature_item in item['FEATURES']:
                    if feature_item['key'] == featureKey and feature_item['key_table'].has_key(subFeatureKey):
                        feature_items = {}
                        st_ed = location_proc(feature_item['location'])
                    
                        if st_ed != None:
                            feature_items['meta_id'] = item['_id']
                            feature_items['meta_ACCESSION'] = item['ACCESSION']
                            feature_items['feature_key'] = featureKey+"."+subFeatureKey
                            feature_items['feature_value'] = feature_item['key_table'][subFeatureKey]
                            
                            kkk = featureKey+"."+subFeatureKey+'='+feature_item['key_table'][subFeatureKey]
                            
                            feature_items['seq_start'] = st_ed[0]
                            feature_items['seq_end'] = st_ed[1]
                            
                            length_sum = 0
                            for i in range(0, len(st_ed[0])):
                                length_sum = length_sum + (st_ed[1][i] - st_ed[0][i] + 1)
                            
                            feature_items['seq_length'] = length_sum
                            
                            new_id = col_idtable.find_and_modify(query={'_id':'features'}, update={'$inc':{'current_id':1}}, new=True).get('current_id')
                            
                            feature_items['_id'] = new_id
                            
                            if st_ed[0] == 0 and st_ed[1] == -1:
                                appendFile(str(new_id)+','+item['ACCESSION'] + ',' + kkk+'\n', logfile)
                        
                            if len(insert_array) >= 1000:
                                insert_array.append(feature_items)
                                col_features.insert(insert_array)
                                
                                insert_array = []
                            else:
                                insert_array.append(feature_items)
                    
#                            check_id = check_exist(col_features, feature_items['meta_id'], feature_items['feature_key'], feature_items['feature_value'])
#                        
#                            if check_id == None:
#                                new_id = col_idtable.find_and_modify(query={'_id':'features'}, update={'$inc':{'current_id':1}}, new=True).get('current_id')
#                                feature_items['_id'] = new_id
#                            
#                                if len(insert_array) >= 1000:
#                                    insert_array.append(feature_items)
#                                    col_features.insert(insert_array)
#                                    
#                                    insert_array = []
#                                else:
#                                    insert_array.append(feature_items)
#                            else:
#                                feature_items['_id'] = check_id
#                            
#                                col_features.update({'_id': check_id}, feature_items)

                            c = c + 1
                            if c % 5000 == 0:
                                print str(c)
                                
        if len(insert_array) != 0:
            col_features.insert(insert_array)
    except Exception, e:
        print 'Error @ _id: ' + str(int(item_id)) + ' '+ kkk
        timeStr = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        col_errs.insert({'item_id':item_id, 'ctime':timeStr, 'event': 'features_length'})
        print e
        traceback.print_exc()
    finally:
        conn_mongo.close()

if __name__ == '__main__':
    start_ms = datetime.datetime.now()
    
    mongo_host = '192.168.40.81'
    mongo_port = 29025
    mongo_db = 'ncbi_data'
    logfile = '/home/shaojing/ncbi/pickup_gene_20130513.log'
    
    conn_mongo = pymongo.Connection(mongo_host, mongo_port)
    
    readInstance()

    end_ms = datetime.datetime.now()
    print 'cost: ' + str((end_ms - start_ms).seconds) + ' s'
    pass