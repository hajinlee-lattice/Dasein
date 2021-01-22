package com.latticeengines.datafabric.entitymanager.impl;

import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntity.PRIMARY_KEY;
import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntity.SORT_KEY;
import static com.latticeengines.datafabric.entitymanager.impl.TestDynamoEntityMgrImpl.RECORD_TYPE;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;

public class TestDynamoEntityMgrTestNG extends DataFabricFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TestDynamoEntityMgrTestNG.class);

    private TestDynamoEntityMgrImpl entityMgr;

    @Inject
    private DynamoService dynamoService;

    private String tableName;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    @BeforeClass(groups = "dynamo")
    public void setup() throws Exception {

        String repo = "TestDynamoEntityMgrTestNG_" + leEnv + "_" + leStack;
        tableName = DynamoDataStoreImpl.buildTableName(repo, RECORD_TYPE);
        dynamoService.deleteTable(tableName);

        dynamoService.createTable(tableName, 10, 10, PRIMARY_KEY, ScalarAttributeType.S.name(), SORT_KEY,
                ScalarAttributeType.S.name(), null);
        ListTablesResult result = dynamoService.getClient().listTables();
        log.info("Tables: " + result.getTableNames());

        entityMgr = new TestDynamoEntityMgrImpl(dataService, repo);
        entityMgr.init();

    }

    @AfterClass(groups = "dynamo")
    public void teardown() throws Exception {
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "dynamo")
    public void testCreateFindDelete() throws Exception {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        attributes.put("key3", 123L);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(objectMapper.writeValueAsString(attributes));

        TestDynamoEntity entity = new TestDynamoEntity();
        entity.setPrimaryId("12345");
        entity.setSortId("23456");
        entity.setJsonAttributes(jsonNode);
        entity.setMapAttributes(attributes);
        System.out.println(objectMapper.writeValueAsString(entity));

        entityMgr.create(entity);

        TestDynamoEntity entity2 = entityMgr.findByKey("12345#23456");
        System.out.println(objectMapper.writeValueAsString(entity2));

        Assert.assertEquals(entity.getId(), entity2.getId());
        Assert.assertEquals(entity2.getJsonAttributes().get("key3").asLong(), 123L);
        Assert.assertEquals(entity2.getJsonAttributes().get("key3").asInt(), 123);
        Assert.assertEquals(entity2.getMapAttributes().get("key3"), 123);
    }

}
