import os
from testbase import TestBase
from leframework.model.states.modelpredictorgenerator import ModelPredictorGenerator


class ModelPredictorExtractionTest(TestBase):
    def testMapBinaryValue(self):
        generator = ModelPredictorGenerator()
        predictor = dict()
        yesInput = ["TrUE", "t", "YeS", "t", "c", "D", "1"]
        noInput = ["FaLSE", "F", "No", "N", "0"]
        naInput = ["", "NA", "N/A", "-1", "NuLL", "NOT AvAILABLE"]
        otherInput = ["dump1", "name"]
        allInput = yesInput + noInput + naInput
        predictor["FundamentalType"] = None
        for value in allInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, value)
        predictor["FundamentalType"] = "year"
        for value in allInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, value)

        predictor["FundamentalType"] = "boolEAn"
        for value in yesInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, 'Yes')
        for value in noInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, 'No')
        for value in naInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, 'Not Available')
        for value in otherInput:
            result = generator._mapBinaryValue(predictor, value)
            self.assertEqual(result, value)

    def mergePredictorElements(self):
        generator = ModelPredictorGenerator()
        buckets = []
        buckets.append({"Count": 2, "Lift": 0.5})
        buckets.append({"Count": 4, "Lift": 0.7})

        mergedBucket = generator._mergePredictorElements(buckets, 0.5)
        self.assertEqual(mergedBucket["Count"], 6)
        self.assertEqual(int(mergedBucket["Lift"] * 100), 63)
