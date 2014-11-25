'''
Created on Oct 23, 2014

@author: hliu
'''
import unittest
from rest.restclient import sendGetRequest, sendPutRequest

class RestClientTest(unittest.TestCase):


    def testSendGetRequest(self):
        response = sendGetRequest("multiply/3/5", "localhost:8080")
        self.assertEqual("multiply", response["operation"])
        self.assertEqual(response["result"], 15)
        
        
    def testSendPostRequest(self):
        request = {"operation":"subtract", "left":5, "right":3}
        response = sendPutRequest(request, "post", "localhost:8080")
        self.assertEqual("subtract", response["operation"])
        self.assertEqual(response["result"], 2)

