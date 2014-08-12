import pickle
import os
from testbase import TestBase
from leframework import scoringengine

class ScoringEngineTest(TestBase):
    inputFileName = "scoringtestinput.txt"
    outputFileName = "scoringtestoutput.txt"
    rowId = "b31d609b-f735-4283-9a33-ff83e7ca3b1f"
    
    def testGetRowToScore(self):
        with open(self.inputFileName) as f:
            line = f.readline()
            rowId, rowDict = scoringengine.getRowToScore(line)
            self.assertEqual(rowId, self.rowId)
            self.assertTrue(rowDict.__contains__('BankruptcyFiled'))
        f.close()
        
    def testGenerateScore(self):
        pickleFile = 'STPipelineBinary.p'
        pipeline = pickle.load(open(pickleFile, "rb"))
        scoringengine.generateScore(pipeline, self.inputFileName, self.outputFileName)
        with open(self.outputFileName) as f:
            line = f.readline()
            result = line.split(',')
            self.assertEqual(result[0], self.rowId)
            self.assertEqual(len(result), 2) 
        f.close()
        if os.path.exists(self.outputFileName):
            os.remove(self.outputFileName)
        
