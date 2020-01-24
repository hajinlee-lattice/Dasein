package com.latticeengines.datafabric.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.datafabric.DynamoAttributes;
import com.latticeengines.domain.exposed.datafabric.DynamoBucketKey;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.DynamoRangeKey;
import com.latticeengines.domain.exposed.datafabric.DynamoStampKey;
import com.latticeengines.domain.exposed.datafabric.TimeSeriesFabricEntity;

public final class DynamoUtil {

    protected DynamoUtil() {
        throw new UnsupportedOperationException();
    }

    public static final String KEYS = "DYNAMOKEYS";
    public static final String ATTRIBUTES = "DYNAMOATTRIBUTES";

    private static final String REPO = "_REPO_";
    private static final String RECORD = "_RECORD_";

    public static String buildTableName(String repository, String recordType) {
        return REPO + repository + RECORD + recordType;
    }

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

        Field keyField = findFieldAnnotatedBy(entityClass, DynamoHashKey.class);
        if (keyField != null) {
            DynamoHashKey annotation = keyField.getAnnotation(DynamoHashKey.class);
            hashKeyAttr = annotation.name();
            hashKeyField = annotation.field();
            if (StringUtils.isBlank(hashKeyField)) {
                hashKeyField = keyField.getName();
            }
        }

        keyField = findFieldAnnotatedBy(entityClass, DynamoRangeKey.class);
        if (keyField != null) {
            DynamoRangeKey annotation = keyField.getAnnotation(DynamoRangeKey.class);
            rangeKeyAttr = annotation.name();
            rangeKeyField = annotation.field();
            if (StringUtils.isBlank(rangeKeyField)) {
                rangeKeyField = keyField.getName();
            }
        }

        keyField = findFieldAnnotatedBy(entityClass, DynamoBucketKey.class);
        if (keyField != null) {
            bucketKeyField = keyField.getName();
        }

        keyField = findFieldAnnotatedBy(entityClass, DynamoStampKey.class);
        if (keyField != null) {
            stampKeyField = keyField.getName();
        }

        return new DynamoIndex(hashKeyAttr, hashKeyField, rangeKeyAttr, rangeKeyField,
                               bucketKeyField, stampKeyField);
    }

    private static Field findFieldAnnotatedBy(Class<?> entityClass, Class<? extends Annotation> annotationCls) {
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(entityClass, annotationCls);
        if (CollectionUtils.isNotEmpty(fields)) {
            return fields.get(0);
        } else {
            return null;
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
