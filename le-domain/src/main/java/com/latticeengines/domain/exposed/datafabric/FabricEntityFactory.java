package com.latticeengines.domain.exposed.datafabric;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroReflectionUtils;

public final class FabricEntityFactory {

    private static final Logger log = LoggerFactory.getLogger(FabricEntityFactory.class);

    public static <T> Schema getFabricSchema(Class<T> clz, String recordType) {
        try {
            if (FabricEntity.class.isAssignableFrom(clz)) {
                T entity = clz.newInstance();
                return ((FabricEntity<?>) entity).getSchema(recordType);
            } else {
                return ReflectData.get().getSchema(clz);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get avro schema of type " + clz.getSimpleName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromFabricAvroRecord(GenericRecord record, Class<T> clz, Schema schema) {
        if (FabricEntity.class.isAssignableFrom(clz)) {
            T entity = null;
            try {
                entity = clz.newInstance();
            } catch (Exception e) {
                log.error("Failed to construct FabricEntity of type " + clz.getSimpleName(), e);
                return null;
            }
            return (T) ((FabricEntity<?>) entity).fromFabricAvroRecord(record);
        } else {
            try {
                return AvroReflectionUtils.fromGenericRecord(record);
            } catch (Exception e) {
                log.error("Failed to convert entity to generic record", e);
                return null;
            }
        }
    }

    public static <T> void setTags(T entity, Map<String, Object> tags) {
        if (tags != null && entity instanceof FabricEntity<?>) {
            for (Map.Entry<String, Object> entry : tags.entrySet()) {
                if (entry.getValue() != null) {
                    ((FabricEntity<?>) entity).setTag(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromHdfsAvroRecord(GenericRecord record, Class<T> clz) {
        if (FabricEntity.class.isAssignableFrom(clz)) {
            T entity = null;
            try {
                entity = clz.newInstance();
            } catch (Exception e) {
                log.error("Failed to construct FabricEntity of type " + clz.getSimpleName(), e);
                return null;
            }
            return (T) ((FabricEntity<?>) entity).fromHdfsAvroRecord(record);
        } else {
            try {
                return AvroReflectionUtils.fromGenericRecord(record);
            } catch (Exception e) {
                log.error("Failed to convert entity to generic record", e);
                return null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTypeParameterClass(Type type) {
        ParameterizedType paramType = (ParameterizedType) type;
        Type actualType = paramType.getActualTypeArguments()[0];
        if (actualType.getTypeName().equals("T")) {
            return null;
        }
        return (Class<T>) actualType;
    }

    public static <T> Pair<GenericRecord, Map<String, Object>> entityToPair(T entity, String recordType,
            Schema schema) {
        try {
            if (entity instanceof FabricEntity) {
                GenericRecord record = ((FabricEntity<?>) entity).toFabricAvroRecord(recordType);
                Map<String, Object> tags = ((FabricEntity<?>) entity).getTags();
                return Pair.of(record, tags);
            }
            log.info("Create Entity " + entity + "Schema " + schema.toString());
            return Pair.of(AvroReflectionUtils.toGenericRecord(entity, schema), null);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record", e);
            return null;
        }
    }

    public static <T> T pairToEntity(Pair<GenericRecord, Map<String, Object>> pair, Class<T> entityClass,
            Schema schema) {
        if (pair == null || pair.getLeft() == null) {
            return null;
        }
        GenericRecord record = pair.getLeft();
        Map<String, Object> tags = pair.getRight();
        T entity = fromFabricAvroRecord(record, entityClass, schema);
        FabricEntityFactory.setTags(entity, tags);
        return entity;
    }

    public static <T> Pair<GenericRecord, Map<String, Object>> entityToPair(T entity, Class<T> clazz,
            String recordType) {
        try {
            if (entity instanceof FabricEntity) {
                GenericRecord record = ((FabricEntity<?>) entity).toFabricAvroRecord(recordType);
                Map<String, Object> tags = ((FabricEntity<?>) entity).getTags();
                return Pair.of(record, tags);
            }
            Schema schema = FabricEntityFactory.getFabricSchema(clazz, recordType);
            log.debug("Create Entity " + entity + "Schema " + schema.toString());
            return Pair.of(AvroReflectionUtils.toGenericRecord(entity, schema), null);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record", e);
            return null;
        }
    }
}
