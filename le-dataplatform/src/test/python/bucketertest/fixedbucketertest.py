from bucketertestbase import BucketerTestBase
from testbase import TestBase

class FixedBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.setup()

    def testBucketing(self): 
        params = {'maxBuckets': 10}
        bandsList = self.bucketColumns('fixed', params)
        for bands in bandsList:
            self.assertTrue(len(bands) <= params['maxBuckets'] + 1)
            for band in bands:
                self.assertTrue(band is not None)

