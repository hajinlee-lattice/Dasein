package com.latticeengines.common.exposed.modeling;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ModelExtractor {
    private static final Logger log = LoggerFactory.getLogger(ModelExtractor.class);

    public void extractModelArtifacts(String modelFilePath, String targetDir) {
        extractModelArtifacts(modelFilePath, targetDir, (dir, name) -> true);
    }

    public List<Pair<String, String>> extractModelArtifacts(InputStream modelFileIs, FilenameFilter filter) {
        List<Pair<String, String>> pairs = new ArrayList<>();
        JsonNode jsonNode = JsonUtils.deserialize(modelFileIs, JsonNode.class);
        ArrayNode fileNodes = (ArrayNode) JsonUtils.tryGetJsonNode(jsonNode, "Model", "CompressedSupportFiles");
        if (fileNodes != null) {
            for (JsonNode fileNode : fileNodes) {
                String fileName = fileNode.get("Key").asText();
                if (filter.accept(null, fileName)) {
                    pairs.add(Pair.of(fileName, fileNode.get("Value").asText()));
                }
            }
        }
        return pairs;
    }

    public void extractModelArtifacts(String modelFilePath, String targetDir, FilenameFilter filter) {
        log.info(String.format("Extracting %s into %s", modelFilePath, targetDir));
        List<Pair<String, String>> entries = new ArrayList<>();
        try {
            FileInputStream is = new FileInputStream(new File(modelFilePath));
            entries.addAll(extractModelArtifacts(is, filter));
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse model.json", e);
        }

        for (Pair<String, String> entry: entries) {
            try {
                FileUtils.write(new File(targetDir + "/" + entry.getKey()), decodeValue(entry.getValue()),
                        Charset.defaultCharset());
            } catch (IOException e) {
                throw new RuntimeException("Failed to write model artifact file " + entry.getKey() //
                        + " to local path " + targetDir);
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
