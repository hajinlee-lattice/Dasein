package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class JsonUtils {

    protected JsonUtils() {
        throw new UnsupportedOperationException();
    }

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

    public static <K, V> List<Map<K, V>> convertListOfMaps(List<?> raw, Class<K> elementKeyClazz,
            Class<V> elementValueClazz) {
        if (raw == null) {
            return null;
        }
        List<Map<K, V>> output = new ArrayList<>();
        for (Object elt : raw) {
            output.add(convertMap((Map<?, ?>) elt, elementKeyClazz, elementValueClazz));
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

    public static JsonNode tryGetJsonNode(JsonNode root, String... paths) {
        JsonNode toReturn = root;
        for (String path: paths) {
            if (toReturn.has(path)) {
                toReturn = toReturn.get(path);
            } else {
                return null;
            }
        }
        return toReturn;
    }

    public static Integer parseIntegerValueAtPath(JsonNode root, String... paths) {
        return parseValueAtPath(root, Integer.class, paths);
    }

    public static String parseStringValueAtPath(JsonNode root, String... paths) {
        return parseValueAtPath(root, String.class, paths);
    }

    private static <T> T parseValueAtPath(JsonNode root, Class<T> clz, String... paths) {
        JsonNode node = tryGetJsonNode(root, paths);
        if (node == null || node instanceof NullNode) {
            return null;
        } else {
            switch (clz.getSimpleName()) {
                case "String":
                    //noinspection unchecked
                    return (T) node.asText();
                case "Integer":
                    //noinspection unchecked
                    return (T) Integer.valueOf(node.asInt());
                default:
                    throw new UnsupportedOperationException("Unknown json type " + clz);
            }
        }
    }

    public static ObjectNode createObjectNode() {
        ObjectMapper mapper = getObjectMapper();
        return mapper.createObjectNode();
    }

    // Converts a JSON object stored as a String in a system resource file to a
    // "Plain Old Java Object" of the
    // provided class.
    // resourceJsonFileRelativePath should start "com/latticeengines/...".
    public static <T> T pojoFromJsonResourceFile(String resourceJsonFileRelativePath, Class<T> clazz)
            throws IOException {
        T pojo = null;
        try {
            InputStream jsonInputStream = ClassLoader.getSystemResourceAsStream(resourceJsonFileRelativePath);
            if (jsonInputStream == null) {
                throw new IOException("Failed to convert resource file " + resourceJsonFileRelativePath
                        + " to InputStream.  Please check path");
            }
            pojo = JsonUtils.deserialize(jsonInputStream, clazz);
            if (pojo == null) {
                String jsonString = IOUtils.toString(jsonInputStream, Charset.defaultCharset());
                throw new IOException(
                        "POJO was null. Failed to deserialize InputStream containing string: " + jsonString);
            }
        } catch (IOException e1) {
            throw new IOException("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath,
                    e1);
        } catch (IllegalStateException e2) {
            throw new IOException("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath,
                    e2);
        }
        return pojo;
    }
}
