package com.latticeengines.aws.dynamo;

import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public interface DynamoItemService {

    void putItem(String tableName, Item item);

    void batchWrite(String tableName, List<Item> items);

    List<Item> batchGet(String tableName, List<PrimaryKey> primaryKeys);

    List<Item> query(String tableName, QuerySpec spec);

}
