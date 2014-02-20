package com.latticeengines.dataplatform.util;

import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;

public class JsonHelper {

    public static <T> String serialize(T object) {
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
}
