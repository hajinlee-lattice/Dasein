import os
from testbase import TestBase


class ModelPredictorExtractiontTest(TestBase):
    def testMapBinaryValue(self):
        
        os.symlink("../resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py", "modelpredictorextraction.py")
        execfile("modelpredictorextraction.py", globals())
        predictor = dict()
        yesInput = ["TrUE", "t", "YeS", "t", "c", "D", "1"]
        noInput = ["FaLSE", "F", "No", "N", "0"]
        naInput = ["", "NA", "N/A", "-1", "NuLL", "NOT AvAILABLE"]
        otherInput = ["dump1", "name"]
        allInput = yesInput + noInput + naInput
        for value in allInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, value)
        predictor["FundamentalType"] = "year"
        for value in allInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, value)
            
        predictor["FundamentalType"] = "boolEAn"
        for value in yesInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, 'Yes')
        for value in noInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, 'No')
        for value in naInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, 'Not Available')
        for value in otherInput:
            result = globals()["mapBinaryValue"](predictor, value)
            self.assertEqual(result, value)