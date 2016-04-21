package com.latticeengines.transform.v2_0_25.common;

import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonParser;
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
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(),
                    clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(String jsonStr, Class<T> clazz,
            Boolean allowUnquotedFieldName) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        if (allowUnquotedFieldName == true)
            objectMapper
                    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS,
                true);
        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(),
                    clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

}
