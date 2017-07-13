package com.latticeengines.datafabric.connector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.FabricMessageServiceImplFunctionalTestNG;
import com.latticeengines.datafabric.service.message.impl.SampleStreamProc;
import com.latticeengines.datafabric.service.message.impl.SimpleFabricMessageConsumerImpl;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class ErrorEntityToHdfsConnectorTestNG extends DataFabricFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FabricMessageServiceImplFunctionalTestNG.class);

    private String topic;
    private String recordType;
    private Schema valueSchema;
    int messageCount = 0;
    int invalidMessages = 0;

    @BeforeMethod(groups = "manual")
    public void setUp() throws Exception {
        topic = "connect-test";
        recordType = "errorRecord";
        // messageService = new FabricMessageServiceImpl("localhost:9092",
        // "localhost:2181",
        // "http://localhost:8081",
        // "yyangdev");
        // topic = "test-topic" + getUUID();

        // if (messageService.topicExists(topic)) {
        // messageService.deleteTopic(topic, false);
        // }
        messageService.createTopic(topic, TopicScope.PRIVATE, 1, 1);

        buildValueSchema();
    }

    @AfterMethod(groups = "manual")
    public void tearDown() throws Exception {
        // if (messageService.topicExists(topic)) {
        // messageService.deleteTopic(topic, false);
        // }
    }

    @Test(groups = "manual", enabled = true)
    public void testProduceConsume() throws Exception {
        FabricMessageProducer producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder(). //
                messageService(messageService). //
                topic(topic). //
                scope(TopicScope.PRIVATE));

        SampleStreamProc processor = new SampleStreamProc();
        FabricMessageConsumer consumer = new SimpleFabricMessageConsumerImpl(
                new SimpleFabricMessageConsumerImpl.Builder(). //
                        messageService(messageService). //
                        group("testGroup"). //
                        topic(topic). //
                        scope(TopicScope.PRIVATE). //
                        processor(processor). //
                        numThreads(1));
        RecordKey key = new RecordKey.Builder().id("1").customerSpace("c").recordType(recordType).producer("producer")
                .build();

        for (int i = 0; i < 16; i++) {
            GenericRecord value = new GenericData.Record(valueSchema);
            value.put("LineNumber", Long.valueOf(i));
            value.put("Id", "id" + i);
            value.put("ErrorMessage", "error" + i);

            producer.send(key, value);
        }

        try {
            Thread.sleep(4000); // 1000 milliseconds is one second.
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        consumer.stop(120000);
        Assert.assertEquals(processor.messageCount, 16);
        Assert.assertEquals(processor.invalidMessages, 0);

    }

    private void buildValueSchema() {
        String schemaString = "{\"namespace\": \"error_value\", \"type\": \"record\", " + "\"name\": \"sample_value\","
                + "\"fields\": [" + "{\"name\": \"LineNumber\", \"type\": \"long\"},"
                + "{\"name\": \"Id\", \"type\": \"string\"}," + "{\"name\": \"ErrorMessage\", \"type\": \"string\"}"
                + "]}";

        System.out.println(schemaString);
        Schema.Parser parser = new Schema.Parser();
        valueSchema = parser.parse(schemaString);
    }
}
