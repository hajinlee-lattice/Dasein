package com.latticeengines.common.exposed.modeling;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

public class ModelExtractor {
    private static final Log log = LogFactory.getLog(ModelExtractor.class);

    public void extractModelArtifacts(String modelFilePath, String targetDir) {
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
                log.warn(e);
            }
        }
        
        for (Map.Entry<String, String> entry : entries) {
            try {
                FileUtils.write(new File(targetDir + "/" + entry.getKey()), entry.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private String decodeValue(String value) throws IOException {
        int BUFFERSIZE = 1000;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] decoded = Base64.decodeBase64(value);
        try (GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(new ByteArrayInputStream(decoded))) {
            byte[] data = new byte[BUFFERSIZE];
            while (gzipInputStream.read(data, 0, BUFFERSIZE) != -1) {
                baos.write(data);
            }
        }
        return new String((baos.toByteArray()));
    }
}
