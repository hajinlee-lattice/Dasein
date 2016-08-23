package com.latticeengines.datafabric.service.datastore.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-context.xml" })
public class DynamoDataStoreImplTestNG extends AbstractTestNGSpringContextTests {


    private AmazonDynamoDBClient client = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000");

    private static final String REPO = "testRepo";
    private static final String RECORD_TYPE = "testRecord";

    private DynamoDataStoreImpl dataStore;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        teardown();

        String tableName = DynamoDataStoreImpl.buildTableName(REPO, RECORD_TYPE);
        CreateTableRequest request = createTable(tableName, 40, 40, "Id", ScalarAttributeType.S.name(), null, null);
        DynamoDB dynamoDB = new DynamoDB(client);
        dynamoDB.createTable(request);
        ListTablesResult result = client.listTables();
        System.out.println("Tables: " + result.getTableNames());
    }
    @AfterClass(groups = "dynamo")
    public void teardown() {
        String tableName = DynamoDataStoreImpl.buildTableName(REPO, RECORD_TYPE);
        if (client.listTables().getTableNames().contains(tableName)) {
            client.deleteTable(tableName);
        }
    }

    @Test(groups = "dynamo")
    public void testCreateDelete() {
        Schema schema = new Schema.Parser().parse(String.format("{\"type\":\"record\",\"name\":\"%s\",\"doc\":\"Testing data\","
                + "\"fields\":[" + "{\"name\":\"ID\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Value\",\"type\":[\"string\",\"null\"]}" + "]}", RECORD_TYPE));

        dataStore = new DynamoDataStoreImpl(client, REPO, RECORD_TYPE, schema);

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
