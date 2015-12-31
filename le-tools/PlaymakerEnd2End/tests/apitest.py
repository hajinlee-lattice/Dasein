'''
Created on Jul 14, 2015

@author: smeng
'''
import unittest
import requests
import time
import threading
import json
from PlaymakerEnd2End.tools import apitool
from Libs.dbtool import SqlServerClient



class Test(unittest.TestCase):

    token = None
    apiUrl = apitool.apiUrl
    tenantUrl = apitool.tenantUrl
    oauthUrl = apitool.oauthUrl


    @classmethod
    def setUpClass(cls):
        tenant = 'TestAPI_DB'
        key = apitool.getOneTimeKey(tenant, "jdbc:sqlserver://10.41.1.193\\SQL2008R2;databaseName=TestAPI_DB")
        cls.token = 'bearer ' + apitool.getAccessToken(key, tenant)
        # print "Key is: " + key
        # print  "token is: " + cls.token


    '''
    There are 471 preleads in the DB. Out of the total 471 preleads, 130 are inactive, 1 does not have status 2800 and 1 does not have synchrozization_destination
    as SFDC. So there should be 339 preleads that can be synchronized.
    
    FUTURE NOTE: this can be handled better. Rather than comparing the results with hardcoded number, it can compared against DB query. 
    
    '''
    def testGetRecommendationCount(self):
        url = self.apiUrl + "/recommendationcount"
        headers = {'Authorization':self.token}

        # first request, start time = 0
        params = {'start':'0', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'], 339)

        # second request, start time = 1445738581
        params = {'start':'1445738581', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'], 136)


    def testGetRecommendation(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1445738581

        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'1000', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(apitool.getRecordCount(request.text), 136)
        self.assertGreaterEqual(apitool.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(apitool.getEndTimestamp(request.text), startTime)



    def testGetRecommendationOffset(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1445738581

        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'100', 'maximum':'1000', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(apitool.getRecordCount(request.text), 36)
        self.assertGreaterEqual(apitool.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(apitool.getEndTimestamp(request.text), startTime)


    def testGetRecommendationRowLimit(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1445738581

        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'99', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(apitool.getRecordCount(request.text), 99)
        self.assertGreaterEqual(apitool.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(apitool.getEndTimestamp(request.text), startTime)



    '''
    There are 29 plays in the DB. Out of the total 29 plays, 1 is inactive, 9 are invisible. So there should be 19 plays that can be synchronized.
    
    FUTURE NOTE: this can be handled better. Rather than comparing the results with hardcoded number, it can compared against DB query. 
    
    '''
    def testGetPlayCount(self):
        url = self.apiUrl + "/playcount"
        headers = {'Authorization':self.token}

        # first request, start time = 0
        params = {'start':'0'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        # print 'Play count: ' + request.text
        self.assertEqual(json.loads(request.text)['count'], 19)

        # second request, start time = 1445738581
        params = {'start':'1445738581'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        # print 'Play count: ' + request.text
        self.assertEqual(json.loads(request.text)['count'], 9)


    def testGetPlay(self):
        url = self.apiUrl + "/plays"
        startTime = 0

        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'1000'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        # print 'Plays: ' + request.text

        self.assertEqual(apitool.getRecordCount(request.text), 19)
        self.assertGreaterEqual(apitool.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(apitool.getEndTimestamp(request.text), startTime)




    threadCount = 0

    def testGetRecommendationMultipleThreads(self):
        for i in range(0, 3):
            t = threading.Thread(target=self.requestRecommendationSingleThread, args=([i]))
            t.daemon = True
            t.start()

        x = 1
        while x < 60:
            if self.threadCount == 3:
                print("time used: ", x)
                return
            x += 1
            time.sleep(1)
        self.fail('Not all threads finished, number of threads finished is {}'.format(self.threadCount))


    # called by each thread
    def requestRecommendationSingleThread(self, i):
        i += 1
        print 'starting within the thread {} at time {}'.format(i, time.ctime())
        startTime = 1445738581
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'99', 'destination':'SFDC'}
        request = requests.get(self.apiUrl + "/recommendations", headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(apitool.getRecordCount(request.text), 99)
        self.assertGreaterEqual(apitool.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(apitool.getEndTimestamp(request.text), startTime)
        self.threadCount += 1
        print 'finishing within the thread {} at time {}'.format(i, time.ctime())


    def testGetRecommendationMultipleTenants(self):
        # get token for another tenant
        tenant = 'TestAPI_DB2'
        key = apitool.getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.193\\SQL2008R2;databaseName=TestAPI_DB2')
        secondToken = 'bearer ' + apitool.getAccessToken(key, tenant)

        url = self.apiUrl + "/recommendationcount"
        params = {'start':'0', 'destination':'SFDC'}


        # first request, for first tenant
        headers = {'Authorization':self.token}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'], 339)

        # second request, for second tenant
        headers = {'Authorization':secondToken}
        request2 = requests.get(url, headers=headers, params=params)
        assert request2.status_code == 200
        self.assertEqual(json.loads(request2.text)['count'], 175)


    def testNomalizedScore(self):

        ''' test that if prelead.[Normalized_Score] score exists, return it as likelyhood, if not exist, return prelead.[Likelihood] '''

        # get token for tenant
        tenant = 'TestAPI_DB_75'
        key = apitool.getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.193\\SQL2008R2;databaseName=TestAPI_DB_75')
        newToken = 'bearer ' + apitool.getAccessToken(key, tenant)

        # use api to request one prelead
        url = self.apiUrl + "/recommendations"
        params = {'start':'0', 'offset':'0', 'maximum':'1', 'destination':'SFDC'}
        request_old = requests.get(url, headers={'Authorization':self.token}, params=params)
        request_new = requests.get(url, headers={'Authorization':newToken}, params=params)
        assert request_old.status_code == 200
        assert request_new.status_code == 200

        prelead_id_old = str(json.loads(request_old.text)['records'][0]['ID'])
        prelead_id_new = str(json.loads(request_new.text)['records'][0]['ID'])
        likelyhood_old_from_api = json.loads(request_old.text)['records'][0]['Likelihood']
        likelyhood_new_from_api = json.loads(request_new.text)['records'][0]['Likelihood']

        # query the database using the preleadId
        db_old = SqlServerClient(server='10.41.1.193\SQL2008R2', database='TestAPI_DB', user='playmaker', password='playmaker')
        db_new = SqlServerClient(server='10.41.1.193\SQL2008R2', database='TestAPI_DB_75', user='playmaker', password='playmaker')
        likelyhood_old_from_db = db_old.queryOne('select * from prelead where prelead_id=' + prelead_id_old).Likelihood
        likelyhood_new_from_db = db_new.queryOne('select * from prelead where prelead_id=' + prelead_id_new).Normalized_Score
        db_old.disconnect()
        db_new.disconnect()

        print 'likelyhood_new_from_api=' + str(likelyhood_new_from_api)
        print 'likelyhood_new_from_db=' + str(likelyhood_new_from_db)
        print 'likelyhood_old_from_api=' + str(likelyhood_old_from_api)
        print 'likelyhood_old_from_db=' + str(likelyhood_old_from_db)

        # compare the value between api and db
        assert likelyhood_new_from_api == likelyhood_new_from_db
        assert likelyhood_old_from_api == likelyhood_old_from_db

    def testEVPlayRevenue(self):

        ''' test that for an EV play, revenue = Prelead.[Monetary_Value] * Prelead.[Likelihood] / 100 '''

        # get token for tenant
        tenant = 'TestAPI_DB_75'
        key = apitool.getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.193\\SQL2008R2;databaseName=TestAPI_DB_75')
        token = 'bearer ' + apitool.getAccessToken(key, tenant)

        # use api to request one prelead
        url = self.apiUrl + "/recommendations"
        params = {'start':'0', 'offset':'0', 'maximum':'1', 'destination':'SFDC'}
        headers = {'Authorization':token}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200

        monetaryValueFromAPI = float(json.loads(request.text)['records'][0]['MonetaryValue'])
        preleadId = str(json.loads(request.text)['records'][0]['ID'])

        print 'monetary value is: ' + str(monetaryValueFromAPI)
        print 'prelead Id is: ' + preleadId

        # query the database using the preleadId
        db = SqlServerClient(server='10.41.1.193\SQL2008R2', database='TestAPI_DB_75', user='playmaker', password='playmaker')
        record = db.queryOne('select * from prelead where prelead_id=' + preleadId)
        likelyhood = record.Likelihood
        monetaryValueFromDB = record.Monetary_Value
        db.disconnect()

        print 'monetary value from DB: ' + str(monetaryValueFromDB)
        print 'likelyhood from DB: ' + str(likelyhood)

        # compare the value between api and db
        assert monetaryValueFromAPI == float(likelyhood) * float(monetaryValueFromDB) / 100


    def testRevenueBackwordCompatibility(self):

        ''' test that prior EV modeling, revenue = Prelead.[Likelihood] / 100 * Play.[Avg_Revenue_Per_Account] '''

        # use api to request one prelead
        url = self.apiUrl + "/recommendations"
        params = {'start':'0', 'offset':'0', 'maximum':'1', 'destination':'SFDC'}
        headers = {'Authorization':self.token}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200

        monetaryValueFromAPI = float(json.loads(request.text)['records'][0]['MonetaryValue'])
        preleadId = str(json.loads(request.text)['records'][0]['ID'])
        playId = str(json.loads(request.text)['records'][0]['PlayID'])

        print 'monetary value is: ' + str(monetaryValueFromAPI)
        print 'prelead Id is: ' + preleadId

        # query the database using the preleadId
        db = SqlServerClient(server='10.41.1.193\SQL2008R2', database='TestAPI_DB', user='playmaker', password='playmaker')
        prelead = db.queryOne('select * from prelead where prelead_id=' + preleadId)
        play = db.queryOne('select * from play where play_id=' + playId)
        likelyhood = prelead.Likelihood
        monetaryValueFromDB = play.Avg_Revenue_Per_Account
        db.disconnect()

        print 'monetary value from DB: ' + str(monetaryValueFromDB)
        print 'likelyhood from DB: ' + str(likelyhood)

        # compare the value between api and db
        assert monetaryValueFromAPI == float(likelyhood) * float(monetaryValueFromDB) / 100

if __name__ == "__main__":
    unittest.main()
