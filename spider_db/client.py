import pymongo
import requests
import json
from spider_db.tongjiju import TongjijuDB,TongjijuTable


My_DBS = {
    'tongjiju':{
        'db_name':'tongjiju_db',
        'meta':'tree_meta',
        'data':'tree_data',
        'desc':'data_desc',
        'table_class':TongjijuTable,
        'db_class':TongjijuDB
    }
}


class SpiderDBClient(object):
    def __init__(self,host='127.0.0.1', port=27017,user=None,passwd=''):
        if user is None:
            self.client = pymongo.MongoClient(host=host, port=port)
        else:
            self.client = pymongo.MongoClient(host=host, port=port,username=user,password=passwd)
    def use_db(self,db_name):
        db_info = My_DBS.get(db_name)
        assert db_info != None
        self.name = db_name
        self.db_info = db_info
        db = db_info['db_class'](self.client,db_info)
        db.Table = db_info['table_class']
        return db
        

