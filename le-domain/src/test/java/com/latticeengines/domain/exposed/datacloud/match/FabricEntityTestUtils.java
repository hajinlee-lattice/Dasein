package com.latticeengines.domain.exposed.datacloud.match;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.BZip2Codec;
import org.apache.avro.file.Codec;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.xerial.snappy.Snappy;

class FabricEntityTestUtils {
    private static final Codec codec = new BZip2Codec();

    static GenericRecord bytesToAvro(byte[] bytes, Schema schema, boolean compression, CompressAlgo algo) {
        try {
            byte[] decompressed;
            if (CompressAlgo.SNAPPY.equals(algo)) {
                decompressed = compression ? Snappy.uncompress(bytes) : bytes;
            } else {
                decompressed = compression ? codec.decompress(ByteBuffer.wrap(bytes)).array() : bytes;
            }
            try (InputStream input = new ByteArrayInputStream(decompressed)) {
                DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                DataInputStream din = new DataInputStream(input);
                Decoder decoder = DecoderFactory.get().binaryDecoder(din, null);
                return reader.read(null, decoder);
            }
        } catch (Exception e) {
            return null;
        }
    }

    static ByteBuffer avroToBytes(GenericRecord record, boolean compression, CompressAlgo algo) {
        Schema schema = record.getSchema();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(record, encoder);
            encoder.flush();
            output.flush();
            if (compression) {
                if (CompressAlgo.SNAPPY.equals(algo)) {
                    return ByteBuffer.wrap(Snappy.compress(output.toByteArray()));
                } else {
                    ByteBuffer raw = ByteBuffer.wrap(output.toByteArray());
                    return codec.compress(raw);
                }
            } else {
                return ByteBuffer.wrap(output.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public enum CompressAlgo {
        BZIP2, SNAPPY
    }
}
