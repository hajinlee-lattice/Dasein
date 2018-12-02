package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtils {

    public static <T> String serialize(T object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();
        StringWriter writer = new StringWriter();
        try {
            objectMapper.writeValue(writer, object);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return writer.toString();
    }

    public static <T> String serialize(T object, Class<?> viewClass) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();
        try {
            return objectMapper.writerWithView(viewClass).writeValueAsString(object);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> void serialize(T object, OutputStream outputStream) {
        if (object == null) {
            return;
        }

        ObjectMapper mapper = JsonUtils.getObjectMapper();
        try {
            mapper.writeValue(outputStream, object);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> T deserialize(InputStream is, Class<T> clazz) {
        if (is == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();

        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(is, clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(String jsonStr, Class<T> clazz) {
        return deserialize(getObjectMapper(), jsonStr, clazz);
    }

    public static <T> T deserializeByTypeRef(String jsonStr, TypeReference<T> type) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();
        try {
            return objectMapper.readValue(jsonStr, type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> T deserialize(ObjectMapper objectMapper, String jsonStr, Class<T> clazz) {
        if (jsonStr == null) {
            return null;
        }
        if (objectMapper == null) {
            objectMapper = getObjectMapper();
        }
        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(), clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(String jsonStr, Class<T> clazz, Boolean allowUnquotedFieldName) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();

        if (allowUnquotedFieldName)
            objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(), clazz);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(InputStream is, TypeReference<T> typeRef) {
        if (is == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();

        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(is, typeRef);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(String jsonStr, TypeReference<T> typeRef) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();

        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr, typeRef);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T deserialize(String jsonStr, TypeReference<T> typeRef, boolean allowSingleQuotes) {
        if (jsonStr == null) {
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();

        if (allowSingleQuotes) {
            objectMapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
        }
        T deserializedSchema;
        try {
            deserializedSchema = objectMapper.readValue(jsonStr.getBytes(), typeRef);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }

    public static <T> T getOrDefault(JsonNode node, Class<T> targetClass, T defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        ObjectMapper mapper = getObjectMapper();

        try {
            return mapper.treeToValue(node, targetClass);
        } catch (JsonProcessingException e) {
            return defaultValue;
        }
    }

    public static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

        return mapper;
    }

    public static <T> T convertValue(Object rawField, Class<T> clazz) {
        return getObjectMapper().convertValue(rawField, clazz);
    }

    public static <T> List<T> convertList(List<?> raw, Class<T> elementClazz) {
        if (raw == null) {
            return null;
        }
        List<T> output = new ArrayList<>();
        for (Object elt : raw) {
            output.add(convertValue(elt, elementClazz));
        }
        return output;
    }

    public static <T> Set<T> convertSet(Set<?> raw, Class<T> elementClazz) {
        if (raw == null) {
            return null;
        }
        Set<T> output = new HashSet<>();
        for (Object elt : raw) {
            output.add(convertValue(elt, elementClazz));
        }
        return output;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> convertMap(Map<?, ?> raw, Class<K> keyClazz, Class<V> valueClazz) {
        if (raw == null) {
            return null;
        }
        Map<K, V> output = new HashMap<>();
        for (Object entry : raw.entrySet()) {
            Map.Entry<Object, Object> casted = (Map.Entry<Object, Object>) entry;
            output.put(convertValue(casted.getKey(), keyClazz), convertValue(casted.getValue(), valueClazz));
        }

        return output;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K, V> Map<K, List<V>> convertMapWithListValue(Map<?, List<?>> raw, Class<K> keyClazz,
            Class<V> listValueClazz) {
        if (raw == null) {
            return null;
        }
        Map<K, List<V>> output = new HashMap<>();
        for (Object entry : raw.entrySet()) {
            Map.Entry<Object, List> casted = (Map.Entry<Object, List>) entry;
            output.put(convertValue(casted.getKey(), keyClazz), convertList(casted.getValue(), listValueClazz));
        }
        return output;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K, V> Map<K, Set<V>> convertMapWithSetValue(Map<?, Set<?>> raw, Class<K> keyClazz,
            Class<V> listValueClazz) {
        if (raw == null) {
            return null;
        }
        Map<K, Set<V>> output = new HashMap<>();
        for (Object entry : raw.entrySet()) {
            Map.Entry<Object, List> casted = (Map.Entry<Object, List>) entry;
            output.put(convertValue(casted.getKey(), keyClazz),
                    new HashSet<>(convertList(casted.getValue(), listValueClazz)));
        }
        return output;
    }

    public static <T> String pprint(T object) {
        try {
            ObjectMapper mapper = getObjectMapper();
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            return object.toString();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T clone(T object) {
        return (T) deserialize(serialize(object), object.getClass());
    }

    public static ObjectNode createObjectNode() {
        ObjectMapper mapper = getObjectMapper();
        return mapper.createObjectNode();
    }
}
