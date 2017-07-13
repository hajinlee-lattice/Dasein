package com.latticeengines.domain.exposed.datafabric;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
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

    @SuppressWarnings("unchecked")
    public static <T> void setTags(T entity, Map<String, Object> tags) {
        if (tags != null && entity instanceof FabricEntity<?>) {
            for (Map.Entry<String, Object> entry: tags.entrySet()) {
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

}
