package com.latticeengines.datafabric.service.message.impl;

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
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class FabricMessageServiceImplFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FabricMessageServiceImplFunctionalTestNG.class);

    private String topic;
    private String recordType;
    private Schema schema;
    int messageCount = 0;
    int invalidMessages = 0;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {
        topic = "demoTopic1";
        recordType = "demoRecord";
        // messageService = new FabricMessageServiceImpl("localhost:9092",
        // "localhost:2181",
        // "http://localhost:8081",
        // "yyangdev");
        // topic = "test-topic" + getUUID();

        // if (messageService.topicExists(topic)) {
        // messageService.deleteTopic(topic, false);
        // }
        messageService.createTopic(topic, TopicScope.PRIVATE, 1, 1);

        buildSchema();
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        // if (messageService.topicExists(topic)) {
        // messageService.deleteTopic(topic, false);
        // }
    }

    @Test(groups = "functional", enabled = false)
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

        for (int i = 0; i < 16; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", "myName" + i);
            record.put("company", "myCompany");
            String id = "yyangdev" + i;
            producer.send(recordType, id, record);
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

    private void buildSchema() {
        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"sample_test\","
                + "\"fields\": [" + "{\"name\": \"name\", \"type\": \"string\"},"
                + "{\"name\": \"company\", \"type\": \"string\"}" + "]}";

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaString);
    }
}
