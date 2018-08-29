package com.latticeengines.domain.exposed.datacloud.match;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LatticeAccountUnitTestNG {

    private static final int NUM_COLUMNS = 1000;
    private static final int REPEAT = 20;

    private static final FabricEntityTestUtils.CompressAlgo algo = FabricEntityTestUtils.CompressAlgo.SNAPPY;

    @Test(groups = "unit")
    public void testDeSer() throws Exception {
        runTest(false);
        Thread.sleep(1000L);
        runTest(true);
    }

    public void runTest(boolean compression) {
        GenericRecord hdfsRecord = getHdfsAvroRecord();
        System.out.println("[compression=" + compression + "] Original byte length in hdfs:  "
                + avroToBytes(hdfsRecord, compression).array().length);

        LatticeAccount account = new LatticeAccount().fromHdfsAvroRecord(hdfsRecord);
        GenericRecord mbusRecord = account.toFabricAvroRecord("LatticeAccount");
        Integer compressedSize = avroToBytes(mbusRecord, compression).array().length;
        Long startTime = System.currentTimeMillis();
        for (int i = 0; i < REPEAT; i++) {
            avroToBytes(mbusRecord, compression);
        }
        Long duration = System.currentTimeMillis() - startTime;
        System.out.println("[compression=" + compression + "] Final byte length in mbus:     " + compressedSize + String
                .format(" Time Elapsed for encoding: %.2f msec", 1.0 * duration / REPEAT));

        byte[] bytes = avroToBytes(mbusRecord, compression).array();
        Schema schema = mbusRecord.getSchema();
        GenericRecord restored = bytesToAvro(bytes, schema, compression);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < REPEAT; i++) {
            bytesToAvro(bytes, schema, compression);
        }
        duration = System.currentTimeMillis() - startTime;

        Assert.assertNotNull(restored);
        Integer compressedSize2 = avroToBytes(restored, compression).array().length;
        System.out.println("[compression=" + compression + "] Final byte length in restored: " + compressedSize2
                + String.format(" Time Elapsed for decoding: %.2f msec", 1.0 * duration / REPEAT));

        Assert.assertEquals(compressedSize, compressedSize2);
        Assert.assertEquals(mbusRecord.get("lattice_account_id").toString(),
                restored.get("lattice_account_id").toString());
    }

    private GenericRecord getHdfsAvroRecord() {
        String schemaStr = "{\"type\":\"record\",\"name\":\"LatticeAccount\",\"doc\":\"Testing data\", \"fields\":[";
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < NUM_COLUMNS; i++) {
            fields.add(String.format("{\"name\":\"" + columnName(i) + "\",\"type\":[\"string\",\"null\"]}", i));
        }
        schemaStr += StringUtils.join(fields, ",");
        schemaStr += ",{\"name\":\"" + LatticeAccount.LATTICE_ACCOUNT_ID_HDFS + "\",\"type\":[\"string\",\"null\"]}]}";
        Schema schema = new Schema.Parser().parse(schemaStr);

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(LatticeAccount.LATTICE_ACCOUNT_ID_HDFS, UUID.randomUUID().toString());
        for (int i = 0; i < NUM_COLUMNS; i++) {
            builder.set(columnName(i), UUID.randomUUID().toString());
        }
        return builder.build();
    }

    private String columnName(int i) {
        return String.format("LongLongLongLongLongColumnName%05d", i);
    }

    private GenericRecord bytesToAvro(byte[] bytes, Schema schema, boolean compression) {
        return FabricEntityTestUtils.bytesToAvro(bytes, schema, compression, algo);
    }

    private ByteBuffer avroToBytes(GenericRecord record, boolean compression) {
        return FabricEntityTestUtils.avroToBytes(record, compression, algo);
    }
}
