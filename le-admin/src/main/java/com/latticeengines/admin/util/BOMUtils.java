package com.latticeengines.admin.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;

public class BOMUtils {

    private BOMUtils() {
    }

    public static String toString(InputStream ins) throws IOException {
        return IOUtils.toString(new BOMInputStream(ins, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE));
    }

}
