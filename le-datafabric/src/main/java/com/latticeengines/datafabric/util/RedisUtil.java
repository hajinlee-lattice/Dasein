package com.latticeengines.datafabric.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.RedisIndex;

public class RedisUtil {

    public static final String INDEX = "REDISINDEX";

    public static Schema getSchema(Class<?> entityClass) {
        Schema schema = ReflectData.get().getSchema(entityClass);
        String redisIndex = constructIndex(entityClass);
        schema.addProp(INDEX, redisIndex);
        return schema;
    }

    public static Map<String, List<String>> getIndex(Schema schema) {
        String redisIndex = schema.getProp(INDEX);
        return getIndex(redisIndex);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, List<String>> getIndex(String indexString) {
        Map<String, List<String>> indexes = (Map<String, List<String>>) JsonUtils.deserialize(indexString, Map.class);
        return indexes;
    }

    public static String constructIndex(Class<?> entityClass) {
        HashMap<String, List<String>> indexes = new HashMap<String, List<String>>();
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(RedisIndex.class)) {
                RedisIndex annotation = (RedisIndex) field.getAnnotation(RedisIndex.class);
                String[] indexNames = StringUtils.split(annotation.name(), ",");
                for (String indexName : indexNames) {
                    List<String> index = (List<String>) indexes.get(indexName);
                    if (index == null) {
                        index = new ArrayList<String>();
                        indexes.put(indexName, index);
                    }
                    index.add(field.getName());
                }
            }
        }

        for (List<String> index : indexes.values()) {
            Collections.sort(index);
        }
        return JsonUtils.serialize(indexes);
    }
}
