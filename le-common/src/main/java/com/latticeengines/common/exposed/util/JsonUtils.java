package com.latticeengines.common.exposed.util;

import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

    public static <T> String serialize(T object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        try {
            objectMapper.writeValue(writer, object);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return writer.toString();
    }

    public static <T> T deserialize(String jsonStr, Class<T> clazz) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(), clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T getOrDefault(JsonNode node, Class<T> targetClass, T defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.treeToValue(node, targetClass);
        } catch (JsonProcessingException e) {
            return defaultValue;
        }
    }
}
