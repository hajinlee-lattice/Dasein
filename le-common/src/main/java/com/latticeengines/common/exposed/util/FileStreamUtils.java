package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileStreamUtils {
    private static final int EOF = -1;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    public static final long copyInputStreamToHdfsWithoutBomAndReturnRows(Configuration configuration, InputStream inputStream,
            String hdfsPath) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(configuration)) {
            try (OutputStream outputStream = fs.create(new Path(hdfsPath))) {
                return copyLarge(new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE), outputStream);
            }
        }
    }

    public static long copyLarge(InputStream input, OutputStream output)
            throws IOException {
        return copyLarge(input, output, new byte[DEFAULT_BUFFER_SIZE]);
    }

    public static long copyLarge(InputStream input, OutputStream output, byte[] buffer)
            throws IOException {
        int n = 0;
        long rows = 0;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            for (byte b : buffer)
                if (b == '\n')
                    rows++;
        }
        return rows;
    }
}
