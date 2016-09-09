from bucketertestbase import BucketerTestBase
from testbase import TestBase

class UnifiedBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.setup()

    def testBucketing(self): 
        params = {}
        bandsList = self.bucketColumns('unified', params)
        for bands in bandsList:
            print (bands)
            for band in bands:
                print band
