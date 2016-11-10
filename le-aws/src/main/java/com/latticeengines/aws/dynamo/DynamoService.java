package com.latticeengines.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

public interface DynamoService {

    Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType);

    void deleteTable(String tableName);

    AmazonDynamoDBClient getClient();

    void switchToLocal();

    void switchToRemote();

    DynamoDB getDynamoDB();

}
