'''
Created on Feb 25, 2013

@author: root
'''

import sys, string, getopt
import gzip, math, hashlib
import json, datetime

import random
import pymongo, gridfs, MySQLdb


def readInstance():
    global conn_mongo
    
    db_mongo = conn_mongo['ncbi_data']
#    db_mongo = conn_mongo['testgen']
    col_query = db_mongo['query']
    
    sp_par = {}
    
    c = 0
    for item in col_query.find():
        id = -1
        
        data_id = item['_id']
        sci_name = item['SCIENTIFIC_NAME'] 
        pars = item['TAXON_LEVEL'].split('; ')
        par_name = pars[len(pars)-1]
        
        if par_name.endswith('.'):
            par_name = par_name[0:len(par_name)-1]
            
        if sp_par.has_key(sci_name + ',' + par_name):
            id = sp_par[sci_name + ',' + par_name]
        else:
            id = get_taxonid(sci_name, par_name)
            sp_par[sci_name + ',' + par_name] = id
            if len(sp_par) >= 50000:
                sp_par = {}
        
#        item['TAXON_ID'] = id
        
        col_query.update({"_id":data_id},{"$set":{'TAXON_ID':int(id)}})
#        print str(id)
        
        c = c + 1
        if c % 5000 == 0:
            print '>> ' + str(c) + ', ' + str(len(sp_par))
        
def get_taxonid(sci_name, par):
    global conn_mysql
    
    id = -1
    
    try:
#        conn = MySQLdb.connect(host='159.226.13.75', user='root', passwd='csdbphylo334', db='palpp1', port=3306)
        cur = conn_mysql.cursor()
        cur2 = conn_mysql.cursor()
        n = cur.execute('select taxon_id from taxon_name where name = "'+sci_name+'"')
        
        if n == 0:
            return -1 #Not found
        elif n > 1:
            for row in cur.fetchall():
                d_id = row[0]
                n2 = cur2.execute('select name from taxon_name where taxon_id = (select parent_taxon_id from taxon, taxon_name where taxon.taxon_id = taxon_name.taxon_id and taxon.taxon_id = '+str(d_id)+' and name_class="scientific name") and name_class = "scientific name"')
                par_name = cur2.fetchone()[0]
                if par == par_name:
                    return d_id
        else:
            id = cur.fetchone()[0]
        cur2.close()     
        cur.close()
#        conn.close()
    except MySQLdb.Error,e:
        print 'MySQL Error!!!'
        return -2 #MySQL Error
    return id

if __name__ == '__main__':
    start_ms = datetime.datetime.now()
    
    mongo_host = '192.168.40.81'
    mongo_port = 29025
    
    conn_mysql = MySQLdb.connect(host='159.226.13.75', user='root', passwd='csdbphylo334', db='darwintree', port=3306)
    conn_mongo = pymongo.Connection(mongo_host, mongo_port)
    
    readInstance()
    
    conn_mongo.close()
    conn_mysql.close()
    
    end_ms = datetime.datetime.now()
    print 'cost: ' + str((end_ms - start_ms).seconds) + ' s'
    pass
