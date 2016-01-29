import encoder
import numpy as np

from testbase import TestBase

class EncoderTest(TestBase):
    
    def testEncodeNone(self):
        t1 = encoder.encode(None)
        self.assertTrue(isinstance(t1, int))
        
    def testEncodeNan(self):
        t1 = encoder.encode(np.NaN)
        self.assertTrue(isinstance(t1, int))

    def testEncodeInteger(self):
        t1 = encoder.encode(123)
        self.assertEquals(t1, 123)

    def testEncodeFloat(self):
        t1 = encoder.encode(123.0)
        self.assertTrue(t1, 123.0)

    def testEncodeLong(self):
        t1 = encoder.encode(12345678912345)
        self.assertTrue(t1, 12345678912345)

    def testEncodeString(self):
        t1 = encoder.encode('abcdef')
        self.assertTrue(t1, 2534611139)
        