'''
Created on May 14, 2013

@author: root
'''

import sys, string, getopt
import gzip, math, hashlib
import json, datetime

import random, time, fileinput
import pymongo, gridfs, MySQLdb, traceback

from bson import BSON

def location_proc2(location):
    
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
                st.append(float(int(tmp)))
                ed.append(float(int(tmp)))
            else:
                st.append(int(float(tmp.split('..')[0])))
                ed.append(int(float(tmp.split('..')[1])))
                
    if len(st) == len(ed):
        return [st, ed]
    else:
        return None
    

def read_db():
    global mongo_db
    
    conn_mongo = pymongo.Connection('192.168.40.83', 29025)
    db_ncbi_data = conn_mongo[mongo_db]
    col_meta = db_ncbi_data['meta']
    col_features = db_ncbi_data['features']
    item_id = -1
    
    try:
        c = 0
        it_query = {'seq_length':0}
        
        for nolen_item in col_features.find(it_query, timeout=False).sort("_id", pymongo.ASCENDING):
            meta_id = nolen_item['meta_id']
            item_id = nolen_item['_id']
            feature_value = nolen_item['feature_value'].strip()
            
            meta_item = col_meta.find_one({'_id':meta_id})
            if meta_item != None:
                for feature_item in meta_item['FEATURES']:
                    if feature_item['key'] == 'gene' and feature_item['key_table'].has_key('gene') and feature_item['key_table']['gene'] == feature_value:
                        st_ed = location_proc2(feature_item['location'])
                        
                        if st_ed != None:
                            length_sum = 0
                            for i in range(0, len(st_ed[0])):
                                length_sum = length_sum + (st_ed[1][i] - st_ed[0][i] + 1)
                                up_query = {'$set':{'seq_start':st_ed[0], 'seq_end':st_ed[1], 'seq_length':length_sum}}
                                
                                col_features.update({'_id':item_id}, up_query)
            c = c + 1
            if c % 1000 == 0:
                print c   
    except Exception, e:
        print 'Error @ _id:' + str(item_id)
        print e
        traceback.print_exc()
    finally:
        conn_mongo.close()

'''
fix length=0 by read log file
'''
def read_log(filename):
    global mongo_db
    
    conn_mongo = pymongo.Connection('192.168.40.83', 29025)
    db_ncbi_data = conn_mongo[mongo_db]
    col_meta = db_ncbi_data['meta']
    col_features = db_ncbi_data['features']
    cur_line = ''
    
    try:
        c = 0
        for line in fileinput.input(filename):
            cur_line = line
#            print line
            tmp1 = line.split('gene.gene')
            op = tmp1[0].split(',')[0].strip()
            accession = tmp1[0].split(',')[1].strip()
            feature_value = tmp1[1][1:len(tmp1[1])].strip()
            
            query = {"feature_key" : "gene.gene", "feature_value" : feature_value, "meta_ACCESSION" : accession}
            
            features_item = col_features.find_one(query)
            
            if features_item != None:
                meta_item = col_meta.find_one({'ACCESSION':accession})
                for feature_item in meta_item['FEATURES']:
                    if feature_item['key'] == 'gene' and feature_item['key_table'].has_key('gene') and feature_item['key_table']['gene'] == feature_value:
    #                    if feature_item['key_table']['gene'] == feature_value:
                        st_ed = location_proc2(feature_item['location'])
                        
                        if st_ed != None:
                            length_sum = 0
                            for i in range(0, len(st_ed[0])):
                                length_sum = length_sum + (st_ed[1][i] - st_ed[0][i] + 1)
                            up_query = {'$set':{'seq_start':st_ed[0], 'seq_end':st_ed[1], 'seq_length':length_sum}}
                            
                            col_features.update({'_id':features_item['_id']}, up_query)

            c = c + 1
            if c % 1000 == 0:
                print c
    except Exception, e:
        print cur_line
        print e
        traceback.print_exc()
    finally:
        conn_mongo.close()

if __name__ == '__main__':
    start_ms = datetime.datetime.now()
    
#    filename = '/home/jingshao/ncbi_data/genedata/pickup_gene_20130513.log'
    filename = '/home/shaojing/ncbi/pickup_gene_20130513.log'
    mongo_db = 'ncbi_data'
    
    read_db()
    
    
    end_ms = datetime.datetime.now()
    print 'cost: ' + str((end_ms - start_ms).seconds) + ' s'
    pass