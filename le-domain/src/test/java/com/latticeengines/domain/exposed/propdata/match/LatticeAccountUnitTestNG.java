package com.latticeengines.domain.exposed.propdata.match;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

public class LatticeAccountUnitTestNG {

    private static final int NUM_COLUMNS = 1000;

    @Test(groups = "unit")
    public void testDeSer() {
        GenericRecord hdfsRecord = getHdfsAvroRecord();
        System.out.println("Original byte length in hdfs: " + avroToBytes(hdfsRecord).array().length);

        LatticeAccount account = new LatticeAccount().fromHdfsAvroRecord(hdfsRecord);
        GenericRecord mbusRecord = account.toFabricAvroRecord("LatticeAccount");
        System.out.println("Final byte length in mbus:    " + avroToBytes(mbusRecord).array().length);

    }

    private GenericRecord getHdfsAvroRecord() {
        String schemaStr = "{\"type\":\"record\",\"name\":\"LatticeAccount\",\"doc\":\"Testing data\", \"fields\":[";
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < NUM_COLUMNS; i++ ) {
            fields.add(String.format("{\"name\":\"" + columnName(i) + "\",\"type\":[\"string\",\"null\"]}", i));
        }
        schemaStr += StringUtils.join(fields, ",");
        schemaStr += ",{\"name\":\"" + LatticeAccount.LATTICE_ACCOUNT_ID_HDFS + "\",\"type\":[\"string\",\"null\"]}]}";
        Schema schema = new Schema.Parser().parse(schemaStr);

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LatticeAccount.LATTICE_ACCOUNT_ID_HDFS, UUID.randomUUID().toString());
        for (int i = 0; i < NUM_COLUMNS; i++ ) {
            builder.set(columnName(i), UUID.randomUUID().toString());
        }
        return builder.build();
    }

    private String columnName(int i) {
        return String.format("LongLongLongLongLongColumnName%05d", i);
    }

    private ByteBuffer avroToBytes(GenericRecord record) {
        Schema schema = record.getSchema();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            //JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output);
            writer.write(record, encoder);
            encoder.flush();
            output.flush();
            return ByteBuffer.wrap(output.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
