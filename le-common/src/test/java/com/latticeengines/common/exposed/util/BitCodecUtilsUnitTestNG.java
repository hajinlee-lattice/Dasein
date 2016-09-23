package com.latticeengines.common.exposed.util;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitCodecUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testEncodeDecode() throws IOException {
        int[] trueBits = new int[] { 2, 4, 6, 8, 1000, 9999, 99999 };
        String encoded = BitCodecUtils.encode(trueBits);

        Assert.assertTrue(encoded.length() < 4000);

        boolean[] values = BitCodecUtils.decode(encoded, new int[]{ 0, 1, 2, 3, 4, 5, 6, 1000, 9999, 99999 });

        Assert.assertTrue(values[2]);
        Assert.assertTrue(values[6]);
        Assert.assertTrue(values[7]);
        Assert.assertTrue(values[8]);
        Assert.assertTrue(values[9]);

        Assert.assertFalse(values[0]);
        Assert.assertFalse(values[5]);
    }

}
