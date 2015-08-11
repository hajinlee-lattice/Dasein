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
        #Paypal_Obfusc_DB
        tenant = 'PCMTest'
        key = apitool.getOneTimeKey(tenant, "jdbc:sqlserver://10.41.1.193\\SQL2008R2;databaseName=PCM")
        cls.token = 'bearer ' + apitool.getAccessToken(key, tenant)
        #print "Key is: " + key
        #print  "token is: " + cls.token


    def testGetRecommendationCount(self):
        url = self.apiUrl + "/recommendationcount"
        headers = {'Authorization':self.token}
        
        #first request, start time = 0
        params = {'start':'0', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'],3348888) 
        
        #second request, start time = 1436549860
        params = {'start':'1436549860', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'],208)
        

    def testGetRecommendation(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1436549860
        
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'1000','destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(self.getRecordCount(request.text), 208)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)


    def getRecordCount(self, response):
        return len(json.loads(response)['records'])

    def getStartTimestamp(self, response):
        return int(json.loads(response)['startDatetime'])

    def getEndTimestamp(self, response):
        return int(json.loads(response)['endDatetime'])


    def testGetRecommendationOffset(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1436549860
        
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'100', 'maximum':'1000','destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(self.getRecordCount(request.text), 108)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)


    def testGetRecommendationRowLimit(self):
        url = self.apiUrl + "/recommendations"
        startTime = 1436549860
        
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'99','destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(self.getRecordCount(request.text), 99)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)



    
    def testGetPlayCount(self):
        url = self.apiUrl + "/playcount"
        headers = {'Authorization':self.token}
        
        #first request, start time = 0
        params = {'start':'0'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        print 'Play count: ' + request.text
        #self.assertEqual(json.loads(request.text)['count'], 3348888) 
        
        #second request, start time = 1436549860
        params = {'start':'1436549860'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        print 'Play count: ' + request.text
        #self.assertEqual(json.loads(request.text)['count'], 208)
        

    def testGetPlay(self):
        url = self.apiUrl + "/plays"
        startTime = 0
        
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'1000'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        print 'Plays: ' + request.text
        '''
        self.assertEqual(self.getRecordCount(request.text), 208)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)
        '''




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
        startTime = 1436549860
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'99','destination':'SFDC'}
        request = requests.get(self.apiUrl + "/recommendations", headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(self.getRecordCount(request.text), 99)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)
        self.threadCount += 1
        print 'finishing within the thread {} at time {}'.format(i, time.ctime())


    def testGetRecommendationMultipleTenants(self):
        # get token for another tenant
        tenant = 'p83_1'
        key = apitool.getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.83\sql2012std;databaseName=ADEDTBDd72072nK28083n154')
        secondToken = 'bearer ' + apitool.getAccessToken(key, tenant)
        
        url = self.apiUrl + "/recommendationcount"
        params = {'start':'0', 'destination':'SFDC'}
        
        #first request, for first tenant
        headers = {'Authorization':self.token}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'],3348888) 
        
        #second request, for second tenant
        headers = {'Authorization':secondToken}
        request2 = requests.get(url, headers=headers, params=params)
        assert request2.status_code == 200
        self.assertEqual(json.loads(request2.text)['count'],468)

        
if __name__ == "__main__":
    unittest.main()