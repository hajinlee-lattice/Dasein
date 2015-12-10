'''
Created on Jul 14, 2015

@author: smeng
'''
import unittest
import requests
import time
import threading
import json
from tools import apitool



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


if __name__ == "__main__":
    unittest.main()
