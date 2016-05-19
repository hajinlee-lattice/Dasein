package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

import org.testng.annotations.Test;

public class GzipUtilsUnitTestNG {
    @Test(groups = "unit")
    public void testRoundTrip() {
        String text = "Hello World";
        byte[] compressed = GzipUtils.compress(text);
        String decompressed = GzipUtils.decompress(compressed);
        assertEquals(decompressed, text);
    }

    @Test(groups = "unit")
    public void testCopyAndCompressStream() throws UnsupportedEncodingException {
        String text = "Hello World";
        ByteArrayInputStream is = new ByteArrayInputStream(text.getBytes());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        GzipUtils.copyAndCompressStream(is, os);

        byte[] compressed = os.toByteArray();
        assertNotEquals(text, new String(compressed, "UTF-8"));
        String decompressed = GzipUtils.decompress(compressed);
        assertEquals(decompressed, text);
    }
}
