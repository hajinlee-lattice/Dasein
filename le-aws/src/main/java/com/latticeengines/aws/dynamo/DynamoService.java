package com.latticeengines.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.Map;

public interface DynamoService {

    Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType);

    void deleteTable(String tableName);

    boolean hasTable(String tableName);

    AmazonDynamoDB getClient();

    AmazonDynamoDB getRemoteClient();

    void updateTableThroughput(String tableName, long readCapacity, long writeCapacity);

    void tagTable(String tableName, Map<String, String> tags);
}
