package com.latticeengines.common.exposed.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

public class GzipUtils {

    public static byte[] compress(final String str) {
        if ((str == null) || (str.length() == 0)) {
            return null;
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(baos)) {
                gzip.write(str.getBytes("UTF-8"));
                gzip.close();
                return baos.toByteArray();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String decompress(final byte[] compressed) {
        if ((compressed == null) || (compressed.length == 0)) {
            return "";
        }
        if (isCompressed(compressed)) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed)) {
                try (GzipCompressorInputStream gis = new GzipCompressorInputStream(bais)) {
                    return IOUtils.toString(gis);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return new String(compressed);
        }
    }

    public static boolean isCompressed(final byte[] compressed) {
        return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
                && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }

    public static String decompressFileToString(String path) {
        try {
            File file = new File(path);
            try (FileInputStream stream = new FileInputStream(file)) {
                return decompress(IOUtils.toByteArray(stream));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void compressFile(String path) {
        try {
            File file = new File(path);
            File outputFile = new File(path + ".gz");
            try (FileInputStream in = new FileInputStream(file)) {
                try (FileOutputStream ostream = new FileOutputStream(outputFile)) {
                    try (GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(ostream)) {
                        IOUtils.copy(in, gzos);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream decompressStream(InputStream stream) {
        try {
            GzipCompressorInputStream zipStream = new GzipCompressorInputStream(stream);
            return zipStream;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
