package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitCodecUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testEncodeDecode() throws IOException {
        int[] trueBits = new int[] { 2, 4, 6, 8, 1000, 9999, 99990 };
        String encoded = BitCodecUtils.encode(trueBits);

        boolean[] values = BitCodecUtils.decode(encoded, new int[] { 0, 1, 2, 3, 4, 5, 6, 1000, 9999, 99990, 99999 });

        Assert.assertTrue(values[2]);
        Assert.assertTrue(values[6]);
        Assert.assertTrue(values[7]);
        Assert.assertTrue(values[8]);
        Assert.assertTrue(values[9]);

        Assert.assertFalse(values[0]);
        Assert.assertFalse(values[5]);
        Assert.assertFalse(values[10]);
    }

    @Test(groups = "unit")
    public void testCodecOfLong() {
        int lowestBit = 2;
        int numBits = 3;

        // 110
        int value = 6;
        long result = BitCodecUtils.setBits(0, lowestBit, numBits, value);
        // 11000
        // value * 4
        Assert.assertEquals(result, 24L);
        Assert.assertEquals(BitCodecUtils.getBits(result, lowestBit, numBits), value);

        // other bits are irrelevant
        long randomBits = new Random(System.currentTimeMillis()).nextInt(Integer.MAX_VALUE);
        result = BitCodecUtils.setBits(randomBits, lowestBit, numBits, value);
        Assert.assertEquals(BitCodecUtils.getBits(result, lowestBit, numBits), value);

        // bit mask check
        long expected = BitCodecUtils.setBits(0, lowestBit, numBits, value);
        long mask = BitCodecUtils.bitMask(0, lowestBit, numBits);
        Assert.assertEquals(result & mask, expected);

        // double bit mask check: check two values together
        int lowestBit2 = 10;
        int numBits2 = 4;
        int value2 = 16;
        // prepare result
        result = BitCodecUtils.setBits(result, lowestBit2, numBits2, value2);
        // prepare bit mask
        mask = BitCodecUtils.bitMask(0, lowestBit, numBits);
        mask = BitCodecUtils.setBits(mask, lowestBit2, numBits2, value2);
        // prepare expected
        expected = BitCodecUtils.setBits(expected, lowestBit2, numBits2, value2);
        // query
        Assert.assertEquals(result & mask, expected);

    }

}
