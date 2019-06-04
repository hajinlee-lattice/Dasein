package com.latticeengines.aws.dynamo;

import java.util.Map;

import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsResult;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.latticeengines.common.exposed.aws.DynamoOperation;

public interface DynamoService {

    Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType);

    void deleteTable(String tableName);

    boolean hasTable(String tableName);

    AmazonDynamoDB getClient();

    AmazonDynamoDB getRemoteClient();

    DynamoDB getDynamo();

    void updateTableThroughput(String tableName, long readCapacity, long writeCapacity);

    void tagTable(String tableName, Map<String, String> tags);

    void switchToLocal(boolean local);

    TableDescription describeTable(String tableName);

    void enableTableAutoScaling(String tableName, DynamoOperation operation, int minCapacityUnits, int maxCapacityUnits,
            double utilizedTargetPct);

    void disableTableAutoScaling(String tableName, DynamoOperation operation);

    DescribeScalingPoliciesResult describeAutoScalingPolicy(String tableName, DynamoOperation operation);

    DescribeScalableTargetsResult describeScalableTargetsResult(String tableName, DynamoOperation operation);
}
