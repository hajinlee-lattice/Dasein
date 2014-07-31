import pickle
import os
from testbase import TestBase
from leframework import scoringengine

class ScoringEngineTest(TestBase):
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
                result = line.split(',')
                self.assertEqual(result[0], self.rowId)
                self.assertEqual(len(result), 2)
        f.close()
        if os.path.exists(self.outputFileName):
            os.remove(self.outputFileName)
        
