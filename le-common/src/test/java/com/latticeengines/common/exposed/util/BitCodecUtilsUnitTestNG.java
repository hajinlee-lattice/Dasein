package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
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
        // 110
        int value = 6;
        long result = BitCodecUtils.setBits(0, 2, 3, value);
        // 11000
        // value * 4
        Assert.assertEquals(result, 24L);
        Assert.assertEquals(BitCodecUtils.getBits(result, 2, 3), value);

        // other bits are irrelevant
        long randomBits = new Random(System.currentTimeMillis()).nextInt(Integer.MAX_VALUE);
        result = BitCodecUtils.setBits(randomBits, 2, 3, value);
        Assert.assertEquals(BitCodecUtils.getBits(result, 2, 3), value);
    }

}
