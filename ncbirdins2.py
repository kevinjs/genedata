'''
Created on Jan 31, 2013

@author: root
'''

import sys, string, getopt
import gzip, math, hashlib
import json, datetime

import random
import pymongo, gridfs, MySQLdb

from bson import BSON

batch_size = 10000
f = None
current_line = ''
l_num = 0
r_num = 0

'''
Remove serial space in single string line.
Edit by jingshao@cnic.cn 
'''
def rm_sp(s, sep=None):
    return (sep or ' ').join(x for x in s.split(sep))

'''
Print list on JSON format.
Edit by jingshao@cnic.cn
'''
def print_list(objList):
    jsonDumpsIndentStr = json.dumps(objList, indent=1)
    print jsonDumpsIndentStr

'''
Read sequence data
Edit by jingshao@cnic.cn
'''
def read_sequence():
    global l_num
    global current_line
    global f
    
    seq = []
    current_line = f.readline()
    l_num = l_num + 1
    
    while not current_line.startswith('//'):
        tmp = current_line.strip().split(' ')
        seq.append(tmp[1:len(tmp)])
        current_line = f.readline()
        l_num = l_num + 1
    return seq
    
'''
Read features
Edit by jingshao@cnic.cn
'''
def read_features():
    global l_num
    global current_line
    global f
    
    feature = []
    genes = []
    
    current_line = f.readline()
    l_num = l_num + 1
    
    while (not current_line.startswith('FEATURES')) and (not current_line.strip().startswith('/')) and (not current_line.strip().startswith('ORIGIN')):
        tmp_ft = {}
        tmp_ft['key'] = rm_sp(current_line).split(' ')[0]
        tmp_ft['location'] = rm_sp(current_line).split(' ')[1]
        [loc, ft_tab] = read_featureKey(tmp_ft['key'])
        
        if loc != tmp_ft['location'] and loc != '':
            tmp_ft['location'] = loc
        tmp_ft['key_table'] = ft_tab
        
#        tmp_ft['key_table'] = read_featureKey(tmp_ft['key'])
        
        feature.append(tmp_ft)
        if tmp_ft['key_table'].has_key('gene'):
#            Remove duplicate gene key
            if genes.count(tmp_ft['key_table']['gene']) == 0:
                genes.append(tmp_ft['key_table']['gene'])
            
    return [feature, genes]

'''
Read feature table in FEATURES
Edit by jingshao@cnic.cn
'''
def read_featureKey(key):
    global l_num
    global current_line
    global f
    
    ft_table = {}
    
    loc = read_multilines2(key)
    
    while current_line.startswith('                     '):
        if current_line.strip().startswith('/'):
            tmp = current_line.strip().split('=')
            if len(tmp) > 1:
                ft_key = tmp[0][1:len(tmp[0])]
                ft_qual = ''
                if tmp[1][0] == '"':
                    if tmp[1][len(tmp[1])-1] == '"':
                        ft_qual = tmp[1][1:len(tmp[1])-1]
                    else:
                        ft_qual = tmp[1][1:len(tmp[1])]
                else:
                    ft_qual = tmp[1]
                    
                ft_table[ft_key] = ft_qual
            current_line = f.readline()
            l_num = l_num + 1
        else:
            tmp2 = current_line.strip()
            if tmp2[len(tmp2)-1] == '"':
                ft_table[ft_key] = ft_table[ft_key] + tmp2[0:len(tmp2)-1]
            else:
                ft_table[ft_key] = ft_table[ft_key] + tmp2
            current_line = f.readline()
            l_num = l_num + 1
        
    return [loc, ft_table]

'''
Read reference
Edit by jingshao@cnic.cn
'''   
def read_reference():
    global l_num
    global current_line
    global f
    
    ref = {}
    
    ref['REFERENCE'] = current_line.strip().replace('REFERENCE', ' ', 1).strip()
    
    current_line = f.readline()
    l_num = l_num + 1
    
    while current_line.startswith('  '):
        if current_line.strip().startswith('AUTHORS'):
            ref['AUTHORS'] = read_multilines('AUTHORS')
        if current_line.strip().startswith('TITLE'): 
            ref['TITLE'] = read_multilines('TITLE')
        if current_line.strip().startswith('JOURNAL'): 
            ref['JOURNAL'] = read_multilines('JOURNAL')
        if current_line.strip().startswith('CONSRTM'): 
            ref['CONSRTM'] = read_multilines('CONSRTM')
        if current_line.strip().startswith('MEDLINE'): 
            ref['MEDLINE'] = read_multilines('MEDLINE')
        if current_line.strip().startswith('NOTE'): 
            ref['NOTE'] = read_multilines('NOTE')
        if current_line.strip().startswith('PUBMED'): 
            ref['PUBMED'] = read_multilines('PUBMED')
        if current_line.strip().startswith('REMARK'): 
            ref['REMARK'] = read_multilines('REMARK')
        if current_line[2] != ' ':
            return ref
        current_line = f.readline()
        l_num = l_num + 1
    return ref

'''
Read ORGANISM
Edit by jingshao@cnic.cn
'''
def read_organism():
    global l_num
    global current_line
    global f
    
    sci_name = current_line.strip().replace('ORGANISM', ' ', 1).strip()
    
    current_line = f.readline()
    l_num = l_num + 1
    
    taxon_level = ''
    
    while current_line.startswith('     '):
        taxon_level = taxon_level + " " + current_line.strip()            
        current_line = f.readline()
        l_num = l_num + 1
    taxon_level = taxon_level.strip()
    
    return [sci_name, taxon_level]    
    
'''
Read multiple lines, start by Keyword.
Edit by jingshao@cnic.cn
'''
def read_multilines(keyword):
    global l_num
    global current_line
    global f
    
    result_line = current_line.strip().replace(keyword, ' ', 1).strip()
    
    current_line = f.readline()
    l_num = l_num + 1
    
    while current_line.startswith('     '):
        result_line = result_line + " " + current_line.strip()            
        current_line = f.readline()
        l_num = l_num + 1
        
    return rm_sp(result_line)

def read_multilines2(keyword):
    global l_num
    global current_line
    global f
    
    result_line = current_line.strip().replace(keyword, ' ', 1).strip()
    
    current_line = f.readline()
    l_num = l_num + 1
    
    while not current_line.strip().startswith('/'):
        result_line = result_line + " " + current_line.strip()            
        current_line = f.readline()
        l_num = l_num + 1
    return rm_sp(result_line)
    
'''
Read file and insert into Mongodb
Edit by jingshao@cnic.cn
'''
def readFile_test(filename):
    global l_num
    global r_num
    global e_num
    
    global current_line
    global f
    global conn_mongo
    
#    filename = '/home/jingshao/ncbi_data/gbbct5.seq'
#    filename = '/home/jingshao/ncbi_data/data/gbpln29.seq.gz'
    
#    db_mongo = conn_mongo['genbank']
    db_mongo = conn_mongo['ncbi_data']
    col_meta = db_mongo['meta']
    col_query = db_mongo['query']
    col_seq = db_mongo['sequence']
    col_idtable = db_mongo['idtable']
    col_files = db_mongo['files']
    col_errs = db_mongo['errs']
    
    meta = {}
    sets = []
    seq = {}
    query = {}
    sp_par = {}
    
    print 'Read file > ' + filename
    if filename.endswith('.gz'):
        f = gzip.open(filename, 'r')
    else:
        f = open(filename, 'r')
    
    try:
        current_line = f.readline()
        while current_line:
            if l_num >= 9:
    #            Start of one entry
                if current_line.startswith('LOCUS'):
                    r_num = r_num + 1
                    if r_num % 5000 == 0:
                        print r_num
                    meta = {}
                    meta['REFERENCE'] = []
                    meta['LOCUS'] = read_multilines('LOCUS')
                    
                    meta['file'] = filename
                    seq['file'] = filename
                    query['file'] = filename
                    
                if current_line.startswith('DEFINITION'):
                    meta['DEFINITION'] = read_multilines('DEFINITION')
                    query['DEFINITION'] = meta['DEFINITION']
                if current_line.startswith('ACCESSION'):
                    meta['ACCESSION'] = read_multilines('ACCESSION')
                    query['ACCESSION'] = meta['ACCESSION']
                    seq['ACCESSION'] = meta['ACCESSION']  
                if current_line.startswith('VERSION'):
                    meta['VERSION'] = read_multilines('VERSION')
                    query['VERSION'] = meta['VERSION']
                    seq['VERSION'] = meta['VERSION']
                if current_line.startswith('NID'):
                    meta['NID'] = read_multilines('NID')
                if current_line.startswith('PROJECT'):
                    meta['PROJECT'] = read_multilines('PROJECT')
                if current_line.startswith('KEYWORDS'):
                    meta['KEYWORDS'] = read_multilines('KEYWORDS')
                if current_line.startswith('SEGMENT'):
                    meta['SEGMENT'] = read_multilines('SEGMENT')
                if current_line.startswith('SOURCE'):
                    meta['SOURCE'] = read_multilines('SOURCE')
                    query['COMMON_NAME'] = meta['SOURCE']
                if current_line.startswith('  ORGANISM'):
                    [sci_name, taxon_level] = read_organism()
    #                par_name = taxon_level
                    pars = taxon_level.split('; ')
                    par_name = pars[len(pars)-1]
                    if par_name.endswith('.'):
                        par_name = par_name[0:len(par_name)-2]
  
                    if sp_par.has_key(sci_name + ',' + par_name):
                        query['TAXON_ID'] = sp_par[sci_name + ',' + par_name]
                    else:
                        query['TAXON_ID'] = int(get_taxonid(sci_name, par_name))
                        sp_par[sci_name + ',' + par_name] = query['TAXON_ID']
                        if len(sp_par) >= 50000:
                            sp_par = {}

#                    query['TAXON_ID'] = int(get_taxonid(sci_name, par_name))
#                    query['TAXON_ID'] = random.randint(1, 10000)
                    
                    meta['ORGANISM'] = sci_name + '\n' + taxon_level
                    query['SCIENTIFIC_NAME'] = sci_name
                    query['TAXON_LEVEL'] = taxon_level
                if current_line.startswith('REFERENCE'):
                    test_ref = read_reference()
                    
                    meta['REFERENCE'].append(test_ref)
    #                May have another reference, so continue
                    continue
                if current_line.startswith('COMMENT'):
                    meta['COMMENT'] = read_multilines('COMMENT')
                if current_line.startswith('FEATURES'):
                    [features, genes] = read_features()
                    meta['FEATURES'] = features
                    query['GENE'] = genes
                    continue
                if current_line.startswith('BASE COUNT'):
                    meta['BASE COUNT'] = read_multilines('BASE COUNT')
                if current_line.startswith('CONTIG'):
                    meta['CONTIG'] = read_multilines('CONTIG')
                if current_line.startswith('ORIGIN'):
                    meta['ORIGIN'] = current_line.replace('ORIGIN',' ').strip()
                    seq['SEQUENCE'] = read_sequence() 
                if current_line.startswith('//'):
                    new_id = col_idtable.find_and_modify(update={'$inc':{'current_id':1}}, new=True).get('current_id')
#                    new_id = r_num
                    meta['_id'] = new_id
                    seq['_id'] = new_id
                    query['_id'] = new_id
                    
                    bson_meta = BSON.encode(meta)
                    if bson_meta.__sizeof__() < 16777216:
                        bson_seq = BSON.encode(seq)
                        if bson_seq.__sizeof__() < 16777216:
                            col_meta.insert(meta)
                            col_query.insert(query)
                            col_seq.insert(seq)
                        else:
                            r_num = r_num - 1
                            e_num = e_num + 1
                            col_errs.insert({'_id':new_id, 'ACCESSION':query['ACCESSION'], 'file':query['file'], 'reason':'Out_of_Size'})
                    else:
                        r_num = r_num - 1
                        e_num = e_num + 1
                        col_errs.insert({'_id':new_id, 'ACCESSION':query['ACCESSION'], 'file':query['file'], 'reason':'Out_of_Size'})
                        
#                    sets.append(meta)
                    
                    meta = {}
                    seq = {}
                    query = {}
            elif l_num == 7:
                total_tmp = current_line.strip().split(' ')
                total_num = string.atoi(total_tmp[0])
                print 'contains: ' + str(total_num)
                col_files.insert({'_id':filename, 'total':total_num, 'current': 0, 'status':'insert'})
                
            current_line = f.readline()
            l_num = l_num + 1

    except:
        print 'Error @ line: ' + str(l_num)
        print current_line
        errinfo = sys.exc_info()
        print errinfo
        col_files.update({'_id':filename}, {'_id':filename, 'total':total_num, 'current': r_num, 'status':'failed'})
        
#    print_list(sets)
    if total_num == (r_num + e_num):
        col_files.update({'_id':filename}, {'_id':filename, 'total':total_num, 'current': r_num, 'status':'succeed'})
    else:
        col_files.update({'_id':filename}, {'_id':filename, 'total':total_num, 'current': r_num, 'status':'failed'})
    f.close()

def get_taxonid(sci_name, par):
    global conn_mysql
    id = -1
    
    try:
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
    
    opts, args = getopt.getopt(sys.argv[1:], "a:i:b:d:h")
    
    host = ''
    srcfile = ''
    port = -1
    
    for op, value in opts:
        if op == "-a":
            host = value[0:value.rindex(':')]
            port = string.atoi(value[value.rindex(':')+1:len(value)])
        elif op == "-i":
            srcfile = value
        elif op == "-h":
            print "python ncbirdins.py -a host:port -i srcfile"
            print "Create by dysj4099@gmail.com Feb.18 2013"
            sys.exit()
    
    r_num = 0
    l_num = 0
    e_num = 0
    
    conn_mysql = MySQLdb.connect(host='xx.xx.xx.xx', user='xxxx', passwd='xxxx', db='xxxx', port=3306)
    conn_mongo = pymongo.Connection(host, port)

    readFile_test(srcfile)
    
    conn_mongo.close()
    conn_mysql.close()
    
    end_ms = datetime.datetime.now()
    print 'insert: '+ str(r_num) + ' error: ' + str(e_num) + ', cost: ' + str((end_ms - start_ms).seconds) + ' s'
    pass
