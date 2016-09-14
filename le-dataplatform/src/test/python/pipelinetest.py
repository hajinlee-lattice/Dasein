from pipeline.pipeline import getObjectFromJSON
from trainingtestbase import TrainingTestBase

class PipelineTest(TrainingTestBase):

    def testPipelineGetObjectFromJSON(self):
        remediateString = "{\"OverlyPredictiveDS\":[\"FeatureRefSrcBag\", \"Column2ToRemove\"]}"

        returnedObject = getObjectFromJSON(type(list()), remediateString)
        self.assertEqual(type(returnedObject), type(list()))
        self.assertEqual([u'OverlyPredictiveDS'], returnedObject)

        returnedObject = getObjectFromJSON(type(dict()), remediateString)
        self.assertEqual(type(returnedObject), type(dict()))
        self.assertEqual({u'OverlyPredictiveDS': [u'FeatureRefSrcBag', u'Column2ToRemove']}, returnedObject)

        returnedObject = getObjectFromJSON(type(str()), "\"Abvc\"")
        self.assertEqual(type(returnedObject), type(unicode()))
        self.assertEqual("Abvc", returnedObject)
