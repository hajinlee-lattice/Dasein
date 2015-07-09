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
        predictor["FundamentalType"] = None
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
            
    def mergePredictorElements(self):
        os.symlink("../resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py", "modelpredictorextraction.py")
        execfile("modelpredictorextraction.py", globals())
        buckets = []
        buckets.append({"Count": 2, "Lift":0.5})
        buckets.append({"Count": 4, "Lift":0.7})
        
        mergedBucket = globals()["mergePredictorElements"](buckets, 0.5)
        self.assertEqual(mergedBucket["Count"], 6)
        self.assertEqual(int(mergedBucket["Lift"] * 100), 63)
        
        
        