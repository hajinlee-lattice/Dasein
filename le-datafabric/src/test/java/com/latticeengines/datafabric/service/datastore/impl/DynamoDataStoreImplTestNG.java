package com.latticeengines.datafabric.service.datastore.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-context.xml" })
public class DynamoDataStoreImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String RECORD_TYPE = "testRecord";

    private AmazonDynamoDBClient client;

    @Autowired
    private DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    private String tableName;
    private String repo;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        repo = leEnv + "_" + leStack + "_testRepo";
        tableName = DynamoDataStoreImpl.buildTableName(repo, RECORD_TYPE);
        teardown();
        dynamoService.createTable(tableName, 10, 10, "Id", ScalarAttributeType.S.name(), null, null);
        client = dynamoService.getClient();
        ListTablesResult result = client.listTables();
        System.out.println("Tables: " + result.getTableNames());
    }
    @AfterClass(groups = "dynamo")
    public void teardown() {
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "dynamo")
    public void testCreateDelete() {
        Schema schema = new Schema.Parser().parse(String.format("{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\","
                + "\"fields\":[" + "{\"name\":\"ID\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Value\",\"type\":[\"string\",\"null\"]}" + "]}", RECORD_TYPE));

        DynamoDataStoreImpl dataStore = new DynamoDataStoreImpl(client, repo, RECORD_TYPE, schema);

        Object[][] data = new Object[][] {
                {"1", "value1"},
                {"2", "value2"}
        };

        List<String> ids = new ArrayList<>();

        for (Object[] tuple : data) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("ID", tuple[0]);
            builder.set("Value", tuple[1]);
            GenericRecord record = builder.build();
            dataStore.createRecord((String) tuple[0], record);
            ids.add((String) tuple[0]);
        }

        Map<String, GenericRecord> records = dataStore.batchFindRecord(ids);
        Assert.assertEquals(records.size(), data.length);

        GenericRecord record = dataStore.findRecord("1");
        Assert.assertEquals(record.get("Value").toString(), "value1");
    }

    private static CreateTableRequest createTable(
            String tableName, long readCapacityUnits, long writeCapacityUnits,
            String partitionKeyName, String partitionKeyType,
            String sortKeyName, String sortKeyType) {

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        keySchema.add(new KeySchemaElement()
                .withAttributeName(partitionKeyName)
                .withKeyType(KeyType.HASH)); //Partition key
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(partitionKeyName)
                .withAttributeType(partitionKeyType));

        if (sortKeyName != null) {
            keySchema.add(new KeySchemaElement()
                    .withAttributeName(sortKeyName)
                    .withKeyType(KeyType.RANGE)); //Sort key
            attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(sortKeyName)
                    .withAttributeType(sortKeyType));
        }

        return new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput( new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));
    }

}
