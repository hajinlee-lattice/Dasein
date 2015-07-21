'''
Created on Jul 14, 2015

@author: smeng
'''
import unittest
import requests
import time
import threading
import json
from IPython.core.tests.test_hooks import Fail



class Test(unittest.TestCase):

    token = None
    apiUrl = 'http://testapi.lattice-engines.com:8080/playmaker'
    tenantUrl = "http://testapi.lattice-engines.com:8080/tenants"
    oauthUrl = "http://testoauth.lattice-engines.com:8080/oauth/token"


    @classmethod
    def getOneTimeKey(cls, tenant, jdbcUrl):
        request = requests.post(cls.tenantUrl,
                          json={"TenantName":tenant,"TenantPassword":"null","ExternalId":tenant,"JdbcDriver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                "JdbcUrl":jdbcUrl,"JdbcUserName":"playmaker","JdbcPassword":"playmaker"})
        assert request.status_code == 200
        response = json.loads(request.text)
        assert response['TenantPassword'] != None
        return response['TenantPassword']

    @classmethod
    def getToken(cls, key, tenant):
        params = {'grant_type':'password', 'username':tenant, 'password':key}
        headers = {'Authorization':'Basic cGxheW1ha2VyOg=='}
        request = requests.post(cls.oauthUrl, params=params, headers=headers)
        assert request.status_code == 200
        assert json.loads(request.text)['access_token'] != None
        return json.loads(request.text)['access_token']


    @classmethod
    def setUpClass(cls):
        tenant = 'PCM'
        key = cls.getOneTimeKey(tenant, "jdbc:sqlserver://10.41.1.82\\sql2012std;databaseName=PCM")
        cls.token = 'bearer ' + cls.getToken(key, tenant)
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
        return int(json.loads(response)['start'])

    def getEndTimestamp(self, response):
        return int(json.loads(response)['end'])


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



    '''
    def testGetPlayCount(self):
        url = self.apiUrl + "/playcount"
        headers = {'Authorization':self.token}
        
        #first request, start time = 0
        params = {'start':'0'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'], 3348888) 
        
        #second request, start time = 1436549860
        params = {'start':'1436549860', 'destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(json.loads(request.text)['count'], 208)
        

    def testGetPlay(self):
        url = self.apiUrl + "/plays"
        startTime = 1436549860
        
        headers = {'Authorization':self.token}
        params = {'start':startTime, 'offset':'0', 'maximum':'1000','destination':'SFDC'}
        request = requests.get(url, headers=headers, params=params)
        assert request.status_code == 200
        self.assertEqual(self.getRecordCount(request.text), 208)
        self.assertGreaterEqual(self.getStartTimestamp(request.text), startTime)
        self.assertGreaterEqual(self.getEndTimestamp(request.text), startTime)
    '''




    threadCount = 0

    def testGetRecommendationMultipleThreads(self):
        for i in range(0, 3):
            t = threading.Thread(target=self.requestRecommendationSingleThread, args=({i+1}))
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
        key = self.getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.83\sql2012std;databaseName=ADEDTBDd72072nK28083n154')
        secondToken = 'bearer ' + self.getToken(key, tenant)
        
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