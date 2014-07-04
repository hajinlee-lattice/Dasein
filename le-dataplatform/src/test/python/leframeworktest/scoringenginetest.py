'''
Created on Jul 2, 2014

@author: hliu
'''
from unittest import TestCase
from leframework import scoringengine
import pickle
import filecmp
import json
import os
from random import random
from sklearn.ensemble import RandomForestClassifier


from launcher import Launcher
from leframework import scoringengine as se



class ScoringEngineTest(TestCase):
    inputFileName = "scoringtestinput.txt"
    outputFileName = "scoringtestoutput.txt"
    rowId = "736e7a75-b38a-4738-828f-92b698b00806"
    
    def testGetRowToScore(self):
        with open(self.inputFileName) as f:
            for line in f:
                rowId, rowDict = scoringengine.getRowToScore(line)
                self.assertEqual(rowId, self.rowId)
                self.assertTrue(rowDict.__contains__('BankruptcyFiled'))
        f.close()
        
    def testGenerateScore(self):
        pickleFile = 'STPipelineBinary.p'
        pipeline = pickle.load(open(pickleFile, "rb"))
        scoringengine.generateScore(pipeline, self.inputFileName, self.outputFileName)
        with open(self.outputFileName) as f:
            for line in f:
                list = line.split(',')
                self.assertEqual(list[0], self.rowId)
                self.assertEqual(len(list), 2)
        f.close()
