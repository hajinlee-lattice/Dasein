package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@Service("dynamoItemService")
public class DynamoItemServiceImpl implements DynamoItemService {

    private static final Logger log = LoggerFactory.getLogger(DynamoItemServiceImpl.class);
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(60);

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
    public List<Item> scan(String tableName, ScanSpec scanSpec) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(scanSpec);
        List<Item> items = new ArrayList<>();
        DynamoDB dynamoDB = dynamoService.getDynamo();
        ItemCollection<ScanOutcome> itemCollection = dynamoDB.getTable(tableName).scan(scanSpec);
        for (Item anItemCollection : itemCollection) {
            items.add(anItemCollection);
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
    public boolean deleteItem(@NotNull String tableName, @NotNull PrimaryKey key) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(key);
        DeleteItemSpec spec = new DeleteItemSpec()
                .withPrimaryKey(key)
                // return all old attributes to determine whether item is deleted
                .withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemOutcome result = delete(tableName, spec);
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getDeleteItemResult());
        return MapUtils.isNotEmpty(result.getDeleteItemResult().getAttributes());
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
            Pair<String, String> keys = findTableKeys(dynamoDB, tableName);
            List<Item> batch = new ArrayList<>();
            for (Item item : items) {
                batch.add(item);
                if (batch.size() >= 25) {
                    submitBatchWrite(dynamoDB, tableName, keys.getLeft(), keys.getRight(), batch);
                    batch.clear();
                }
            }
            if (CollectionUtils.isNotEmpty(batch)) {
                submitBatchWrite(dynamoDB, tableName, keys.getLeft(), keys.getRight(), batch);
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

    private Pair<String, String> findTableKeys(DynamoDB dynamoDB, String tableName) {
        Table table = dynamoDB.getTable(tableName);
        TableDescription description = table.getDescription();
        String hashKey = "";
        String rangeKey = "";
        for (KeySchemaElement schema: description.getKeySchema()) {
            KeyType keyType = KeyType.fromValue(schema.getKeyType());
            if (KeyType.HASH.equals(keyType)) {
                hashKey = schema.getAttributeName();
                log.info("Found hash key of table " + tableName + " to be " + hashKey);
            } else if (KeyType.RANGE.equals(keyType)) {
                rangeKey = schema.getAttributeName();
                log.info("Found range key of table " + tableName + " to be " + rangeKey);
            }
        }
        return Pair.of(hashKey, rangeKey);
    }

    /**
     * At most 25 items
     */
    private void submitBatchWrite(DynamoDB dynamoDB, String tableName, //
                                  String partitionKey, String rangeKey, List<Item> items) {
        if (items.size() > 25) {
            throw new IllegalArgumentException(
                    "Can only put at most 25 items in one batch, but attempting to put " + items.size());
        }
        TableWriteItems writeItems = new TableWriteItems(tableName);
        items.forEach(writeItems::addItemToPut);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(writeItems);
            long startTime = System.currentTimeMillis();
            int backoff = 15;
            do {
                try {
                    // Check for unprocessed keys which could happen if you exceed
                    // provisioned throughput
                    Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
                    if (outcome.getUnprocessedItems().size() != 0) {
                        List<String> ids = extractIdsFromUnprocessItems(unprocessedItems.get(tableName), //
                                partitionKey, rangeKey);
                        log.info("Resubmit unprocessed items: " + StringUtils.join(ids, ", "));
                        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                    }
                } catch (ProvisionedThroughputExceededException e) {
                    log.warn("Exceeded provisioned throughput, retry after " + backoff + " seconds.");
                    try {
                        Thread.sleep(backoff * 1000);
                        backoff = Math.min(backoff * 2, 240);
                    } catch (InterruptedException e2) {
                        log.warn("Sleep interrupted.", e2);
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

    private List<String> extractIdsFromUnprocessItems(List<WriteRequest> writeRequests, //
                                                      String partitionKey, String rangeKey) {
        List<String> ids = new ArrayList<>();
        writeRequests.forEach(writeRequest -> {
            if (writeRequest.getPutRequest() != null) {
                Map<String, AttributeValue> item = writeRequest.getPutRequest().getItem();
                String id = "";
                if (item.containsKey(partitionKey)) {
                    try {
                        id += item.get(partitionKey).getS();
                    } catch (Exception e) {
                        log.warn("Failed to retrieve partition key " + partitionKey + " as type S from put request");
                    }
                }
                if (item.containsKey(rangeKey)) {
                    try {
                        String rangeVal = item.get(rangeKey).getS();
                        if (StringUtils.isNotBlank(rangeVal)) {
                            id += ":" + rangeVal;
                        }
                    } catch (Exception e) {
                        log.warn("Failed to retrieve range key " + rangeKey + " as type S from put request");
                    }
                }
                if (StringUtils.isNotBlank(id)) {
                    ids.add(id);
                } else {
                    ids.add("UNKNOWN");
                }
            }
        });
        return ids;
    }

}
