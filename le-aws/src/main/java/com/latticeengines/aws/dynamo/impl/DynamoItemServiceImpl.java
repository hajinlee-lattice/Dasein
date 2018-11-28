package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;

@Service("dynamoItemService")
public class DynamoItemServiceImpl implements DynamoItemService {

    private static final Logger log = LoggerFactory.getLogger(DynamoItemServiceImpl.class);
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(30);

    private final DynamoService dynamoService;

    @Inject
    public DynamoItemServiceImpl(DynamoService dynamoService) {
        this.dynamoService = dynamoService;
    }

    @Override
    public List<Item> query(String tableName, QuerySpec spec) {
        List<Item> items = new ArrayList<>();

        try (PerformanceTimer timer = new PerformanceTimer()) {
            DynamoDB dynamoDB = dynamoService.getDynamo();
            ItemCollection<QueryOutcome> itemCollection = dynamoDB.getTable(tableName).query(spec);
            for (Item anItemCollection : itemCollection) {
                items.add(anItemCollection);
            }
            timer.setTimerMessage("Queried " + items.size() + " items from table " + tableName);
        }

        return items;
    }

    @Override
    public Item getItem(@NotNull String tableName, @NotNull PrimaryKey key) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(key);
        DynamoDB dynamoDB = dynamoService.getDynamo();
        return dynamoDB.getTable(tableName).getItem(key);
    }

    @Override
    public void putItem(String tableName, Item item) {
        DynamoDB dynamoDB = dynamoService.getDynamo();
        dynamoDB.getTable(tableName).putItem(item);
    }

    @Override
    public PutItemOutcome put(@NotNull String tableName, @NotNull PutItemSpec putItemSpec) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(putItemSpec);
        DynamoDB dynamoDB = dynamoService.getDynamo();
        return dynamoDB.getTable(tableName).putItem(putItemSpec);
    }

    @Override
    public UpdateItemOutcome update(@NotNull String tableName, @NotNull UpdateItemSpec updateItemSpec) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(updateItemSpec);
        DynamoDB dynamoDB = dynamoService.getDynamo();
        return dynamoDB.getTable(tableName).updateItem(updateItemSpec);
    }


    @Override
    public DeleteItemOutcome delete(@NotNull String tableName, @NotNull DeleteItemSpec deleteItemSpec) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(deleteItemSpec);
        DynamoDB dynamoDB = dynamoService.getDynamo();
        return dynamoDB.getTable(tableName).deleteItem(deleteItemSpec);
    }

    @Override
    public void batchWrite(String tableName, List<Item> items) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DynamoDB dynamoDB = dynamoService.getDynamo();
            List<Item> batch = new ArrayList<>();
            for (Item item : items) {
                batch.add(item);
                if (batch.size() >= 25) {
                    submitBatchWrite(dynamoDB, tableName, batch);
                    batch.clear();
                }
            }
            if (CollectionUtils.isNotEmpty(batch)) {
                submitBatchWrite(dynamoDB, tableName, batch);
            }
            timer.setTimerMessage("Write " + items.size() + " items to table " + tableName);
        }
    }

    @Override
    public List<Item> batchGet(String tableName, List<PrimaryKey> primaryKeys) {
        List<Item> results = new ArrayList<>();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DynamoDB dynamoDB = dynamoService.getDynamo();
            List<PrimaryKey> batch = new ArrayList<>();
            for (PrimaryKey pk: primaryKeys) {
                batch.add(pk);
                if (batch.size() >= 100) {
                    List<Item> batchResult = submitBatchGet(dynamoDB, tableName, batch);
                    results.addAll(batchResult);
                    batch.clear();
                }
            }
            if (CollectionUtils.isNotEmpty(batch)) {
                results.addAll(submitBatchGet(dynamoDB, tableName, batch));
            }
            timer.setTimerMessage("Get " + primaryKeys.size() + " items from table " + tableName);
        }
        return results;
    }

    /**
     * At most 100 keys
     */
    private List<Item> submitBatchGet(DynamoDB dynamoDB, String tableName, List<PrimaryKey> primaryKeys) {
        if (primaryKeys.size() > 100) {
            throw new IllegalArgumentException(
                    "Can only get at most 100 items in one batch, but attempting to get " + primaryKeys.size());
        }
        TableKeysAndAttributes keys = new TableKeysAndAttributes(tableName);
        primaryKeys.forEach(keys::addPrimaryKey);
        List<Item> results = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(keys);
            Map<String, KeysAndAttributes> unprocessed;
            do {
                List<Item> items = outcome.getTableItems().get(tableName);
                results.addAll(items);
                // Check for unprocessed keys which could happen if you exceed
                // provisioned throughput or reach the limit on response size.
                unprocessed = outcome.getUnprocessedKeys();
                if (MapUtils.isNotEmpty(unprocessed)) {
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessed);
                }
            } while (MapUtils.isNotEmpty(unprocessed));
        }
        return results;
    }

    /**
     * At most 25 items
     */
    private void submitBatchWrite(DynamoDB dynamoDB, String tableName, List<Item> items) {
        if (items.size() > 25) {
            throw new IllegalArgumentException(
                    "Can only put at most 25 items in one batch, but attempting to put " + items.size());
        }
        TableWriteItems writeItems = new TableWriteItems(tableName);
        items.forEach(writeItems::addItemToPut);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(writeItems);
            long startTime = System.currentTimeMillis();
            do {
                try {
                    // Check for unprocessed keys which could happen if you exceed
                    // provisioned throughput
                    Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
                    if (outcome.getUnprocessedItems().size() != 0) {
                        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                    }
                } catch (Exception e) {
                    log.error("Unable to batch create records " + tableName, e);
                }

            } while (outcome.getUnprocessedItems().size() > 0 && System.currentTimeMillis() - startTime < TIMEOUT);
            if (outcome.getUnprocessedItems().size() > 0) {
                throw new RuntimeException("Failed to finish a batch write within timeout");
            }
            timer.setTimerMessage("Write a single batch of " + items.size() + " items to table " + tableName);
        }
    }

}
