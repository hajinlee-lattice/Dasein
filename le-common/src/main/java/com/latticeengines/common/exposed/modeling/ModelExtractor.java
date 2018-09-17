package com.latticeengines.common.exposed.modeling;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

public class ModelExtractor {
    private static final Logger log = LoggerFactory.getLogger(ModelExtractor.class);

    public void extractModelArtifacts(String modelFilePath, String targetDir) {
        extractModelArtifacts(modelFilePath, targetDir, (dir, name) -> true);
    }

    public void extractModelArtifacts(String modelFilePath, String targetDir, FilenameFilter filter) {
        log.info(String.format("Extracting %s into %s", modelFilePath, targetDir));
        JsonFactory f = new JsonFactory();
        JsonParser parser = null;
        boolean retrieveSupportFiles = false;
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        try {
            parser = f.createParser(new File(modelFilePath));
            Map.Entry<String, String> entry = null;
            while (parser.nextToken() != null) {
                String fieldName = parser.getCurrentName();
                if ("CompressedSupportFiles".equals(fieldName)) {
                    retrieveSupportFiles = true;
                }

                if (retrieveSupportFiles && "Key".equals(fieldName)) {
                    String value = parser.getText();
                    if (fieldName != null && !"Key".equals(value)) {
                        if (entry == null) {
                            entry = new AbstractMap.SimpleEntry<String, String>(value, null);
                            entries.add(entry);
                        }
                    }
                }
                if (retrieveSupportFiles && "Value".equals(fieldName)) {
                    String value = parser.getText();
                    if (fieldName != null && !"Value".equals(value)) {
                        entry.setValue(decodeValue(value));
                        entry = null;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                parser.close();
            } catch (IOException e) {
                log.warn("Failed to close JsonParser!");
            }
        }

        for (Map.Entry<String, String> entry : entries) {
            try {
                if (filter != null && !filter.accept(null, entry.getKey())) {
                    continue;
                } else {
                    FileUtils.write(new File(targetDir + "/" + entry.getKey()), entry.getValue(),
                            Charset.defaultCharset());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String decodeValue(String value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] data = Base64.decodeBase64(value);
            try (GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(
                    new ByteArrayInputStream(data))) {
                IOUtils.copy(gzipInputStream, baos);
                return new String((baos.toByteArray()));
            }
        }
    }
}
