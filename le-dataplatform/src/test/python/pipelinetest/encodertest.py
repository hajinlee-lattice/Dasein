import encoder
import numpy as np

from testbase import TestBase

class EncoderTest(TestBase):
    
    def testEncodeNone(self):
        t1 = encoder.transform(None)
        self.assertTrue(isinstance(t1, int))
        
    def testEncodeNan(self):
        t1 = encoder.transform(np.NaN)
        self.assertTrue(isinstance(t1, int))

    def testEncodeInteger(self):
        t1 = encoder.transform(123)
        self.assertEquals(t1, 123)

    def testEncodeFloat(self):
        t1 = encoder.transform(123.0)
        self.assertTrue(t1, 123.0)

    def testEncodeLong(self):
        t1 = encoder.transform(12345678912345)
        self.assertTrue(t1, 12345678912345)
        