package com.latticeengines.datafabric.util;

import java.lang.reflect.Field;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.DynamoRangeKey;

public class DynamoUtil {

    public static final String ATTRIBUTES = "DYNAMOATTRIBUTES";


    public static DynamoIndex getAttributes(String attrString) {
        if (attrString == null) {
            return null;
        } else {
            return JsonUtils.deserialize(attrString, DynamoIndex.class);
        }
    }

    public static String constructAttributes(Class<?> entityClass) {
        String hashKeyAttr = null;
        String hashKeyField = null;
        String rangeKeyAttr = null;
        String rangeKeyField = null;
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(DynamoHashKey.class)) {
                DynamoHashKey annotation = (DynamoHashKey)field.getAnnotation(DynamoHashKey.class);
                hashKeyAttr = annotation.name();
                hashKeyField = field.getName();
            }
            if (field.isAnnotationPresent(DynamoRangeKey.class)) {
                DynamoRangeKey annotation = (DynamoRangeKey)field.getAnnotation(DynamoRangeKey.class);
                rangeKeyAttr = annotation.name();
                rangeKeyField = field.getName();
            }
        }
        if (hashKeyAttr == null)
            return null;
        else {
            DynamoIndex index = new DynamoIndex(hashKeyAttr, hashKeyField, rangeKeyAttr, rangeKeyField);
            return JsonUtils.serialize(index);
        }
    }
}
