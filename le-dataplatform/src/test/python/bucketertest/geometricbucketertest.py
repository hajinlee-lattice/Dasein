from bucketertestbase import BucketerTestBase
from testbase import TestBase

class GeometricBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.setup()

    def testBucketing(self): 
        params = {'maxBuckets':9, 'minValue': 1, 'multiplierList': [2, 2.5, 2]}
        bandsList = self.bucketColumns('geometric', params)
        for bands in bandsList:
            self.assertTrue(len(bands) <= params['maxBuckets']+1)
            for band in bands:
                self.assertTrue(band is not None)
