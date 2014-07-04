'''
Created on Jul 2, 2014

@author: hliu
'''
from unittest import TestCase
from leframework import scoringengine
import pickle

class ScoringEngineTest(TestCase):
    inputFileName = "scoringtestinput.txt"
    outputFileName = "scoringtestoutput.txt"
    rowId = "68707d3d-b131-44e5-9f68-5e2e65256b41"

    def testGetRowToScore(self):
        with open(self.inputFileName) as f:
            for line in f:
                rowId, rowDict = scoringengine.getRowToScore(line)
                self.assertEqual(rowId, "68707d3d-b131-44e5-9f68-5e2e65256b41")
                self.assertTrue(rowDict.__contains__('BankruptcyFiled'))
                self.assertEqual(rowDict['BankruptcyFiled'], '0.227284523331')
        f.close()
        


    def testGenerateScore(self):
        currentPath = '/home/hliu/workspace/le-dataplatform/src/test/python/results'
        pickleFile = currentPath + '/STPipelineBinary.p'
        pipeline = pickle.load(open(pickleFile, "rb"))
        scoringengine.generateScore(pipeline, self.inputFileName, self.outputFileName)
        with open(self.outputFileName) as f:
            for line in f:
                list = line.split(',')
                self.assertEqual(list[0], self.rowId)
                self.assertEqual(len(list), 2)
        f.close()
