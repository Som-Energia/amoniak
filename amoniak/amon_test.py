#!/usr/bin/env python                                                                                 
        
import unittest    
from datetime import datetime, date                                                                   

import pymongo
import json

import configdb
from .amon import AmonConverter                                                        
from .utils import setup_mongodb, setup_peek
from .tasks import get_last_cchfact_upload, push_amon_measures, push_cchfact

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

    def test_get_one_curve(self):
        data = [{'name':'ES0122000011340380CE0F',
                'create_at':'2018-08-25 03:58:10.216000',
                'datetime':datetime.strptime('2018-09-13T09:00:00Z',"%Y-%m-%dT%H:%M:%SZ"),
                'ai': 1485}]
        self.create_fake_data(self.collection_name, data)
        result = self.amon_converter.cchfact_to_amon('ES0122000011340380CE0F', '2018-08-24 03:58:10.216000')
        data = json.loads(result)
        self.assertEquals(data,
                         {'meteringPointId': 'fa3d9029-1aa4-53d1-953e-c26ebf495cdb',
                         'deviceId': '42f02596-eead-5614-bca4-89f9b2581908',
                         'measurements': [{'consolidated': True,
                         'type': 'electricityConsumption',
                         'value': 1485,
                         'timestamp': '2018-09-13T09:00:00Z'}]})

    def test_get_multiple_curves(self):
        data = [{'name':'ES0122000011340380CE0F',
                'create_at':'2018-08-25 03:58:10.216000',
                'datetime':datetime.strptime('2018-08-13T09:00:00Z',"%Y-%m-%dT%H:%M:%SZ"),
                'ai': 1485},
                {'name':'ES0122000011340380CE0F',
                 'create_at':'2018-08-24 03:58:10.216000',
                 'datetime':datetime.strptime('2018-08-13T09:00:00Z',"%Y-%m-%dT%H:%M:%SZ"),
                 'ai' : 1500},
                {'name':'ES0122000011340380CE0F',
                 'create_at':'2018-08-23 03:58:10.216000',
                 'datetime':datetime.strptime('2018-08-13T09:00:00Z',"%Y-%m-%dT%H:%M:%SZ"),
                 'ai' : 1600},
                {'name':'ES0122000011340380CE0F',
                 'create_at':'2018-08-20 03:58:10.216000',
                 'datetime':datetime.strptime('2018-08-13T09:00:00Z',"%Y-%m-%dT%H:%M:%SZ"),
                 'ai' : 1500}]


        self.create_fake_data(self.collection_name, data)
        result = self.amon_converter.cchfact_to_amon('ES0122000011340380CE0F', '2018-08-21 03:58:10.216000')
        data = json.loads(result)
        self.assertEquals(data,
                         {'meteringPointId': 'fa3d9029-1aa4-53d1-953e-c26ebf495cdb',
                         'deviceId': '42f02596-eead-5614-bca4-89f9b2581908',
                         'measurements':
                           [{'consolidated': True,
                             'type': 'electricityConsumption',
                             'value': 1485,
                             'timestamp': '2018-08-13T09:00:00Z'},
                            {'consolidated': True,
                             'type': 'electricityConsumption',
                             'value': 1500,
                             'timestamp': '2018-08-13T09:00:00Z'},
                            {'consolidated': True,
                             'type': 'electricityConsumption',
                             'value': 1600,
                             'timestamp': '2018-08-13T09:00:00Z'}]})

class tasks_test(unittest.TestCase):

    def setUp(self):
        self.setupMongoDB()

        self.mongo_dbconn = setup_mongodb(**{'host': 'localhost', 'database': self.databasename})
        self.erp_conn = setup_peek(**configdb.erppeek)
        self.amon_converter = AmonConverter(self.erp_conn, self.mongo_dbconn)

    def setupMongoDB(self):
        self.databasename = 'cchfact_test'
        self.collection_name = 'tg_cchfact'
        self.mongo_conn = pymongo.MongoClient('localhost')
        
    def tearDown(self):
        self.mongo_conn.drop_database(self.databasename)

    def create_fake_data(self, collection, data):
        return self.mongo_dbconn[collection].insert_many(data)


    def test_get_last_cchfact_empty(self):
        date = get_last_cchfact_upload()
        print date
        self.assertEquals(date, '2018-10-02 03:45:25')

