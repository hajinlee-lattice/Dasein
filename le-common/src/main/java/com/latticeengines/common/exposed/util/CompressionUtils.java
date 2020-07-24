package com.latticeengines.common.exposed.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompressionUtils {

    protected CompressionUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(CompressionUtils.class);

    private static final int headTotalLengthToCheck = TarConstants.VERSION_OFFSET + TarConstants.VERSIONLEN;

    public static byte[] decompressByteArray(final byte[] input) {
        return decompressByteArray(input, 1024);
    }

    public static byte[] decompressByteArray(final byte[] input, final int bufferLength) {
        if (null == input) {
            throw new IllegalArgumentException("Input was null");
        }

        // Create the decompressor and give it the data to compress
        final Inflater decompressor = new Inflater();

        decompressor.setInput(input);

        // Create an expandable byte array to hold the decompressed data
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length);

        // Decompress the data
        final byte[] buf = new byte[bufferLength];

        try {
            while (!decompressor.finished()) {
                int count = decompressor.inflate(buf);
                baos.write(buf, 0, count);
            }
        } catch (DataFormatException ex) {
            log.error("Problem decompressing.", ex);
        }

        try {
            baos.close();
        } catch (IOException ex) {
            log.error("Problem closing stream.", ex);
        }

        return baos.toByteArray();
    }

    public static byte[] compressByteArray(byte[] input) throws IOException {
        return compressByteArray(input, 1024);
    }

    public static byte[] compressByteArray(byte[] input, int bufferLength) throws IOException {
        // Compressor with highest level of compression
        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_COMPRESSION);

        // Give the compressor the data to compress
        compressor.setInput(input);
        compressor.finish();

        // Create an expandable byte array to hold the compressed data.
        // It is not necessary that the compressed data will be smaller than
        // the uncompressed data.
        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

        // Compress the data
        byte[] buf = new byte[bufferLength];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }

        bos.close();

        // Get the compressed data
        return bos.toByteArray();

    }

    public static void untarInputStream(InputStream inputStream, String destDir) throws IOException {
        if (!destDir.endsWith("/")) {
            destDir += "/";
        }
        BufferedInputStream in = new BufferedInputStream(inputStream);
        GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
        TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);

        TarArchiveEntry entry;
        int BUFFER = 2048;

        while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
            log.info("Extracting: " + entry.getName());
            String destPath = destDir + entry.getName();
            if (entry.isDirectory()) {
                File f = new File(destPath);
                f.mkdirs();
            } else {
                int count;
                byte data[] = new byte[BUFFER];

                File parentDir = new File(new File(destPath).getParent());
                parentDir.mkdirs();

                FileOutputStream fos = new FileOutputStream(destPath);
                BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER);
                while ((count = tarIn.read(data, 0, BUFFER)) != -1) {
                    dest.write(data, 0, count);
                }
                dest.close();
            }
        }
        tarIn.close();
        log.info("Successfully untared into " + destDir);
    }



    public static CompressType getCompressType(InputStream inputStream) {
        try (InputStream inputStream2 = inputStream) {
            int totalLength = TarConstants.VERSION_OFFSET + TarConstants.VERSIONLEN;
            byte[] buffer = new byte[totalLength];
            int offset = 0;
            while (offset < totalLength) {
                offset += inputStream2.read(buffer, offset, totalLength - offset);
            }
            if (GzipUtils.isCompressed(buffer)) {
                return CompressType.GZ;
            } else if (ZipArchiveInputStream.matches(buffer, totalLength)) {
                return CompressType.ZIP;
            } else if (TarArchiveInputStream.matches(buffer, totalLength)) {
                return CompressType.TAR;
            }
            return CompressType.NO_COMPRESSION;
        } catch (IOException e) {
            throw new RuntimeException("Failed to determine to file type!");
        }
    }

    public static InputStream getCompressInputStream(InputStream inputStream, CompressType compressType) throws IOException {
        switch (compressType) {
            case GZ:
                return new GzipCompressorInputStream(inputStream);
            case TAR:
                return new TarArchiveInputStream(inputStream);
            case ZIP:
                return new ZipArchiveInputStream(inputStream);
            case TAR_GZ:
                return new TarArchiveInputStream(new GzipCompressorInputStream(inputStream));
            default:
                return inputStream;
        }
    }

    public static boolean isValidArchiveEntry(ArchiveEntry archiveEntry) {
        if (archiveEntry.isDirectory()) {
            return false;
        }
        String entryName = archiveEntry.getName();
        int index = entryName.lastIndexOf("/");
        // file name should not start with "."
        if (index > 0 && entryName.substring(index).startsWith(".")) {
            return false;
        } else {
            return true;
        }
    }

    public enum CompressType {
        ZIP, GZ, TAR, TAR_GZ, NO_COMPRESSION
    }
}
