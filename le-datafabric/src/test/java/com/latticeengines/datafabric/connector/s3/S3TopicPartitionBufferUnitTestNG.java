package com.latticeengines.datafabric.connector.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;

public class S3TopicPartitionBufferUnitTestNG {

    @Test(groups = "unit")
    public void testCleanConstruct() throws IOException {
        TopicPartition tp = new TopicPartition("topic1", 2);
        Schema schema = new Schema.Parser().parse( //
                "{\"type\":\"record\",\"name\":\"TestRecord\",\"doc\":\"Testing data\","
                + "\"fields\":[" //
                + "{\"name\":\"key\",\"type\":\"string\"}, " //
                + "{\"name\":\"value\",\"type\":\"string\"}]}");
        AvroTopicPartitionBuffer writer = new AvroTopicPartitionBuffer(tp);
        writer.setSchema(schema);

        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (int i = 0; i < 10; i++) {
            builder.set("key", "key" + String.valueOf(i));
            builder.set("value", "value" + String.valueOf(i));
            records.add(builder.build());
        }

        Assert.assertEquals(writer.getRows(), new Integer(0));
        writer.writeDataToFile(records);
        Assert.assertEquals(writer.getRows(), new Integer(10));

        File expected = new File(S3SinkConstants.TMP_ROOT + "/topic1/2.avro");
        Assert.assertTrue(expected.isFile());
        records = AvroUtils.readFromLocalFile(expected.getAbsolutePath());
        for (GenericRecord record: records) {
            String key = record.get("key").toString();
            String value = record.get("value").toString();
            Assert.assertEquals(key.replace("key", "value"), value);
        }

        FileUtils.deleteQuietly(expected);
    }

}
