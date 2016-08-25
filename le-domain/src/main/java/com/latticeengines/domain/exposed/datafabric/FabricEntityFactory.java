package com.latticeengines.domain.exposed.datafabric;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class FabricEntityFactory {

    private static final Log log = LogFactory.getLog(FabricEntityFactory.class);

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
        try {
            if (FabricEntity.class.isAssignableFrom(clz)) {
                T entity = clz.newInstance();
                return (T) ((FabricEntity<?>) entity).fromFabricAvroRecord(record);
            } else {
                T entity = null;
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                ReflectDatumReader<T> reader = new ReflectDatumReader<>(schema);
                ByteArrayOutputStream output = new ByteArrayOutputStream();

                try {
                    Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
                    writer.write(record, encoder);
                    encoder.flush();

                    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
                    Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
                    entity = reader.read(null, decoder);
                } catch (Exception e) {
                    log.error("Failed to convert entity to generic record", e);
                    return null;
                } finally {
                    try {
                        output.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }

                return entity;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct FabricEntity of type " + clz.getSimpleName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromHdfsAvroRecord(GenericRecord record, Class<T> clz) {
        try {
            T entity = clz.newInstance();
            return (T) ((FabricEntity<?>) entity).fromHdfsAvroRecord(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct FabricEntity of type " + clz.getSimpleName(), e);
        }
    }

}
