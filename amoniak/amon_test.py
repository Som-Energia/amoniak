#!/usr/bin/env python                                                                                 
        
import unittest    
from datetime import datetime, date                                                                   

import pymongo

import configdb
from .amon import AmonConverter                                                        
from .utils import setup_mongodb, setup_peek
            


class cchfact_to_amon_test(unittest.TestCase):

    def setUp(self):
        self.setupMongoDB()
        
        self.mongo_dbconn = setup_mongodb(**{'host': 'localhost', 'database': self.databasename})
        erp_conn = setup_peek(**configdb.erppeek)
        self.amon_converter = AmonConverter(erp_conn, self.mongo_dbconn)
    
    def setupMongoDB(self):
        self.databasename = 'cchfact_test'
        self.collection_name = 'tg_cchfact'
        self.mongo_conn = pymongo.MongoClient('localhost')
        
    def tearDown(self):
        self.mongo_conn.drop_database(self.databasename)

    def create_fake_data(self, collection, data):
        return self.mongo_dbconn[collection].insert_many(data)

    def test_get_empty(self):
        data = [{'name':'ES0122000011340380CE0F', 'create_at':'2018-08-24 03:58:10.216000'}]
        self.create_fake_data(self.collection_name, data)
        
        result = self.amon_converter.cchfact_to_amon('ES0122000011340380CE0F', '2019-08-24 03:58:10.216000')
        self.assertDictEqual(result, {})

        
