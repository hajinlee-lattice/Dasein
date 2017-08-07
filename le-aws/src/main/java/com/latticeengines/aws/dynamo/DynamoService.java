package com.latticeengines.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

public interface DynamoService {

    Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType);

    void deleteTable(String tableName);

    AmazonDynamoDB getClient();

    AmazonDynamoDB getRemoteClient();
}
