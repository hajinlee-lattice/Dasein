package com.latticeengines.datafabric.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.util.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.DynamoAttributes;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.DynamoRangeKey;

public class DynamoUtil {

    public static final String KEYS = "DYNAMOKEYS";
    public static final String ATTRIBUTES = "DYNAMOATTRIBUTES";

    public static DynamoIndex getIndex(String keyString) {
        if (keyString == null) {
            return null;
        } else {
            return JsonUtils.deserialize(keyString, DynamoIndex.class);
        }
    }

    public static DynamoAttributes getAttributes(String attrString) {
        if (attrString == null) {
            return null;
        } else {
            return JsonUtils.deserialize(attrString, DynamoAttributes.class);
        }
    }

    public static String constructIndex(Class<?> entityClass) {
        String hashKeyAttr = null;
        String hashKeyField = null;
        String rangeKeyAttr = null;
        String rangeKeyField = null;
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(DynamoHashKey.class)) {
                DynamoHashKey annotation = field.getAnnotation(DynamoHashKey.class);
                hashKeyAttr = annotation.name();
                hashKeyField = field.getName();
            }
            if (field.isAnnotationPresent(DynamoRangeKey.class)) {
                DynamoRangeKey annotation = field.getAnnotation(DynamoRangeKey.class);
                rangeKeyAttr = annotation.name();
                rangeKeyField = field.getName();
            }
        }
        if (hashKeyAttr == null) {
            return null;
        } else {
            DynamoIndex index = new DynamoIndex(hashKeyAttr, hashKeyField, rangeKeyAttr, rangeKeyField);
            return JsonUtils.serialize(index);
        }
    }

    public static String constructAttributes(Class<?> entityClass) {
        List<String> attrs = new ArrayList<>();
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(DynamoAttribute.class)) {
                DynamoAttribute annotation = field.getAnnotation(DynamoAttribute.class);
                if (StringUtils.isEmpty(annotation.value())) {
                    attrs.add(field.getName());
                } else {
                    attrs.add(annotation.value());
                }
            }
        }
        if (attrs.size() == 0) {
            return null;
        } else {
            DynamoAttributes attributes = new DynamoAttributes(attrs);
            return JsonUtils.serialize(attributes);
        }
    }
}
