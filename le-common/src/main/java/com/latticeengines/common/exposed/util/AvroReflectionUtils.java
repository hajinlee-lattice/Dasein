package com.latticeengines.common.exposed.util;

import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public final class AvroReflectionUtils {
    public static <T> GenericRecord toGenericRecord(T entity, Class<T> clazz) {
        return toGenericRecord(entity, ReflectData.get().getSchema(clazz));
    }

    public static <T> GenericRecord toGenericRecord(T entity, Schema schema) {
        try {
            ReflectDatumWriter<T> writer = new ReflectDatumWriter<T>(schema);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);

            writer.write(entity, encoder);
            encoder.flush();

            Decoder decoder = DecoderFactory.get().binaryDecoder(output.toByteArray(), null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to convert class of type %s to avro record",
                    schema.getName()), e);
        }
    }

    public static <T> T fromGenericRecord(GenericRecord record) {
        Schema schema = record.getSchema();
        try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);

            writer.write(record, encoder);
            encoder.flush();

            Decoder decoder = DecoderFactory.get().binaryDecoder(output.toByteArray(), null);
            ReflectDatumReader<T> reader = new ReflectDatumReader<T>(schema);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to convert avro record to class of type %s",
                    schema.getName()), e);
        }
    }
}
