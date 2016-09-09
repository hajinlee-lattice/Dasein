from bucketertestbase import BucketerTestBase
from testbase import TestBase

class UnifiedBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.setup()

    def testBucketing(self): 
        params = {}
        _ = self.bucketColumns('unified', params)