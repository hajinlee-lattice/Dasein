package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class DynamoItemServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String PARTITION_KEY = "PartitionId";
    private static final String SORT_KEY = "SortId";

    @Value("${common.le.environment}")
    private String env;

    @Value("${common.le.stack}")
    private String stack;

    @Inject
    private DynamoService dynamoService;

    @Inject
    private DynamoItemService dynamoItemService;

    private String tableName;

    @BeforeClass(groups = "functional")
    private void setup() {
        tableName = "DynamoItemServiceImplTestNG_" + env + "_" + stack;
        dynamoService.deleteTable(tableName);

        long readCapacityUnits = 10;
        long writeCapacityUnits = 10;
        String partitionKeyType = ScalarAttributeType.S.name();
        String sortKeyType = ScalarAttributeType.S.name();
        dynamoService.createTable(tableName, readCapacityUnits, writeCapacityUnits, PARTITION_KEY, partitionKeyType,
                SORT_KEY, sortKeyType);
    }

    @AfterClass(groups = "functional")
    private void teardown() {
//         dynamoService.deleteTable(tableName);
    }

    @Test(groups = "functional")
    public void testDynamoCrud() throws InterruptedException {
        Assert.assertTrue(true);
        List<Item> itemList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String pid = String.valueOf(i / 100);
            String sid = String.valueOf(i % 100);
            Item item = createItem(pid, sid, String.valueOf(i));
            itemList.add(item);
        }
        dynamoItemService.batchWrite(tableName, itemList);

        List<PrimaryKey> pks = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String pid = String.valueOf(i / 10);
            String sid = String.valueOf(i % 10);
            PrimaryKey pk = new PrimaryKey(PARTITION_KEY, pid, SORT_KEY, sid);
            pks.add(pk);
        }
        List<Item> retrieved = dynamoItemService.batchGet(tableName, pks);
        Assert.assertEquals(retrieved.size(), 100);
    }

    private Item createItem(String pid, String sid, String value) {
        Item item = new Item();
        item.with(PARTITION_KEY, pid);
        item.with(SORT_KEY, sid);
        item.with("Value", value);
        return item;
    }

}
