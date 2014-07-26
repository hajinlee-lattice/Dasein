from bucketertestbase import BucketerTestBase
from unittest import TestCase
import numpy as np

class StandardBucketerTest(TestCase, BucketerTestBase):

    def setUp(self):
        self.setup()
        
    def testBucketing(self): 
        params = {'maxBuckets': 6}
        bandsList = self.bucketColumns('standard', params)
        for bands in bandsList:
            self.assertTrue(len(bands) <= params['maxBuckets']+1)
            for band in bands:
                if band is not None:
                    self.assertFalse(np.isinf(band))
