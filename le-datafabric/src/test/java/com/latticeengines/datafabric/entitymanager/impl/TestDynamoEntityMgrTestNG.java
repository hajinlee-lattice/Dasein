package com.latticeengines.datafabric.entitymanager.impl;

import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntityMgrImpl.RECORD_TYPE;
import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntityMgrImpl.REPO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;

public class TestDynamoEntityMgrTestNG extends DataFabricFunctionalTestNGBase {

    private AmazonDynamoDBClient client = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000");

    private TestDynamoEntityMgrImpl entityMgr;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {
        teardown();

        String tableName = DynamoDataStoreImpl.buildTableName(REPO, RECORD_TYPE);
        CreateTableRequest request = createTable(tableName, 40, 40, "Id", ScalarAttributeType.S.name(), null, null);
        DynamoDB dynamoDB = new DynamoDB(client);
        dynamoDB.createTable(request);
        ListTablesResult result = client.listTables();
        System.out.println("Tables: " + result.getTableNames());

        entityMgr = new TestDynamoEntityMgrImpl(messageService, dataService);
        entityMgr.init();

    }

    @AfterClass(groups = "dynamo")
    public void teardown() {
        String tableName = DynamoDataStoreImpl.buildTableName(REPO, RECORD_TYPE);
        if (client.listTables().getTableNames().contains(tableName)) {
            client.deleteTable(tableName);
        }
    }

    @Test(groups = "dynamo")
    public void testCreateFindDelete() throws  Exception {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        attributes.put("key3", 123L);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(objectMapper.writeValueAsString(attributes));


        TestDynamoEntity entity = new TestDynamoEntity();
        entity.setId("12345");
        entity.setJsonAttributes(jsonNode);
        entity.setMapAttributes(attributes);
        System.out.println(objectMapper.writeValueAsString(entity));

        entityMgr.create(entity);

        TestDynamoEntity entity2 = entityMgr.findByKey("12345");
        System.out.println(objectMapper.writeValueAsString(entity2));

        Assert.assertEquals(entity.getId(), entity2.getId());
        Assert.assertEquals(entity2.getJsonAttributes().get("key3").asLong(), 123L);
        Assert.assertEquals(entity2.getJsonAttributes().get("key3").asInt(), 123);
        Assert.assertEquals(entity2.getMapAttributes().get("key3"), 123);
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
