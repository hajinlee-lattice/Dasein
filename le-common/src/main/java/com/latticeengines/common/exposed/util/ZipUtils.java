package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;

public class ZipUtils {

    public static String decompressFileToString(String path) {
        try {
            ZipFile file = new ZipFile(path);
            Enumeration<ZipArchiveEntry> entries = file.getEntries();
            List<ZipArchiveEntry> entryList = Collections.list(entries);

            if (entryList.size() != 1) {
                throw new RuntimeException("Expected exactly 1 file in the zip file");
            }

            InputStream is = file.getInputStream(entryList.get(0));
            return IOUtils.toString(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream decompressStream(InputStream stream) {
        try {
            ZipArchiveInputStream inputStream = new ZipArchiveInputStream(stream);
            // This positions the stream to the next entry
            ZipArchiveEntry entry = inputStream.getNextZipEntry();
            if (entry == null) {
                throw new RuntimeException("Expected exactly 1 entry in the compressed stream");
            }
            if (entry.isDirectory()) {
                throw new RuntimeException(
                        "Directories not allowed in the input stream.  Only exactly 1 file supported.");
            }
            return inputStream;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
