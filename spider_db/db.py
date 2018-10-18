import pymongo
import requests
import json
        

class Table(object):
    def __init__(self,meta):
        self.meta = meta 
        self.option_dict = None
        self.data = None

    def get_options(self):
        pass

    def fetch_data(self):
        pass


class SpiderDB(object):
    def __init__(self,client,db_info):
        self.client = client
        self.spider_db = self.client[db_info['db_name']]
        #self.spider_db = client['tongjiju_db']
        self.coll_meta = self.spider_db[db_info['meta']]
        self.coll_data= self.spider_db[db_info['data']]

    def docs_readable(self,docs):
        l = list(map(lambda x:(x['query_type'],(x['title'],x['trace_name'],x['id_name']),(x.get('section'),x['trace'],x['id'])),docs))
        c = 0
        for item in l:
            print(item)
            c=c+1
        print('total find=%d'%c)

    def find_table_by_keyword(self,keyword):
        return self.coll_meta.find({ '$or':[{'id_name':{'$regex':keyword}},{'trace_name':{'$regex':keyword}}]})

        

    def all_docs(self):
        return self.coll_meta.find()
    

    def docs_count(self):
        return self.coll_meta.find().count()
    
    def get_table(self,section=None,trace=None,id=None):
        query = []
        if section is not None:
            query.append({'section':section})
        if trace is not None:
            query.append({'trace':trace})
        if id is not None:
            query.append({'id':id})
        data = self.coll_meta.find_one({'$and':query})
        return self.Table(data)
