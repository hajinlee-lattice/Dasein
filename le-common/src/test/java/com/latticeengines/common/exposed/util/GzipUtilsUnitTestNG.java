package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class GzipUtilsUnitTestNG {
    @Test(groups = "unit")
    public void testRoundTrip() {
        String text = "Hello World";
        byte[] compressed = GzipUtils.compress(text);
        String decompressed = GzipUtils.decompress(compressed);
        assertEquals(decompressed, text);
    }
}
