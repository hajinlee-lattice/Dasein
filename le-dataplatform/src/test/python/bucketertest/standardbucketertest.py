from bucketertestbase import BucketerTestBase
from testbase import TestBase

class StandardBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.setup()

    def testBucketing(self): 
        params = {'maxBuckets': 6}
        bandsList = self.bucketColumns('standard', params)
        for bands in bandsList:
            self.assertTrue(len(bands) <= params['maxBuckets']+1)
            for band in bands:
                self.assertTrue(band is not None)
