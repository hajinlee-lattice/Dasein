package com.latticeengines.scoringharness.util;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil {
    public static ObjectNode parseObject(String json) {
        try {
            return (ObjectNode) new ObjectMapper().readTree(json);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to parse %s as json", json), e);
        }
    }

    public static ObjectNode createObject() {
        return (ObjectNode) new ObjectMapper().createObjectNode();
    }
}
