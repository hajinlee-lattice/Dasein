'''
Created on Oct 23, 2014

@author: hliu
'''
import os
from unittest import TestCase
from modeling.modelingjob import ModelingJob

class ModelingJobTest(TestCase):
        
    def testWebHdfs(self):
        self.modelingJob = ModelingJob()
        self.modelingJob.setWebHDFS("http://localhost:50070")
        self.assertEqual(len(self.modelingJob.listFiles("/tmp/modeltest.json")), 0)
        self.modelingJob.addFile("../../resources/jsonschema/model.json", "/tmp/modeltest.json")
        self.modelingJob.getFile("/tmp/modeltest.json", "modeltest.json")
        self.modelingJob.rmDir("/tmp/modeltest.json")
        
    def tearDown(self):
        os.remove("modeltest.json")
