package com.latticeengines.datafabric.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.util.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.DynamoAttributes;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.DynamoRangeKey;
import com.latticeengines.domain.exposed.datafabric.DynamoBucketKey;
import com.latticeengines.domain.exposed.datafabric.DynamoStampKey;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.TimeSeriesFabricEntity;

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
        DynamoIndex index = null;
        if (TimeSeriesFabricEntity.class.isAssignableFrom(entityClass)) {
            DynamoIndex cInd = constructIndexInternal(CompositeFabricEntity.class);
            DynamoIndex bInd = constructIndexInternal(TimeSeriesFabricEntity.class);
            index = new DynamoIndex(cInd.getHashKeyAttr(), cInd.getHashKeyField(),
                                    cInd.getRangeKeyAttr(), cInd.getRangeKeyField(),
                                    bInd.getBucketKeyField(), bInd.getStampKeyField());
        } else if (CompositeFabricEntity.class.isAssignableFrom(entityClass)) {
            index = constructIndexInternal(CompositeFabricEntity.class);
        }  else {
            index = constructIndexInternal(entityClass);
        }

        if (index.getHashKeyAttr() == null) {
            return null;
        } else {
           return JsonUtils.serialize(index);
        }
    }


    public static DynamoIndex constructIndexInternal(Class<?> entityClass) {

        String hashKeyAttr = null;
        String hashKeyField = null;
        String rangeKeyAttr = null;
        String rangeKeyField = null;
        String bucketKeyField = null;
        String stampKeyField = null;
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
            if (field.isAnnotationPresent(DynamoBucketKey.class)) {
                bucketKeyField = field.getName();
            }
            if (field.isAnnotationPresent(DynamoStampKey.class)) {
                stampKeyField = field.getName();
            }
        }

        return new DynamoIndex(hashKeyAttr, hashKeyField, rangeKeyAttr, rangeKeyField,
                               bucketKeyField, stampKeyField);
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
