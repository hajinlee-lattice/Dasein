package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class CompressionUtilUnitTestNG {
    
    @Test(groups = "unit")
    public void compressThenDecompress() throws Exception {
        String uncompressedStr = "This is the uncompressed string.";
        
        byte[] uncompressed = CompressionUtil.decompressByteArray(CompressionUtil.compressByteArray(uncompressedStr.getBytes()));
        
        assertEquals(new String(uncompressed), uncompressedStr);
    }
}
