package com.latticeengines.aws.dynamo;

import java.util.List;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public interface DynamoItemService {

    void putItem(String tableName, Item item);

    Item getItem(String tableName, PrimaryKey key);

    void batchWrite(String tableName, List<Item> items);

    List<Item> batchGet(String tableName, List<PrimaryKey> primaryKeys);

    List<Item> query(String tableName, QuerySpec spec);

    List<Item> scan(@NotNull String tableName, @NotNull ScanSpec scanSpec);

    /*
     * Thin wrapper to expose full put/update/delete functionality
     */

    PutItemOutcome put(@NotNull String tableName, @NotNull PutItemSpec putItemSpec);

    UpdateItemOutcome update(@NotNull String tableName, @NotNull UpdateItemSpec updateItemSpec);

    DeleteItemOutcome delete(@NotNull String tableName, @NotNull DeleteItemSpec deleteItemSpec);
}
