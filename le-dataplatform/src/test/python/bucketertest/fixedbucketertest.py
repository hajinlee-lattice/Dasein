from bucketertestbase import BucketerTestBase
from unittest import TestCase
import numpy as np

class FixedBucketerTest(TestCase, BucketerTestBase):

    def setUp(self):
        self.setup()
        
    def testBucketing(self): 
        params = {'maxBuckets': 10}
        bandsList = self.bucketColumns('fixed', params)
        for bands in bandsList:
            print bands
            self.assertTrue(len(bands) <= params['maxBuckets']+1)
            for band in bands:
                if band is not None:
                    self.assertFalse(np.isinf(band))

