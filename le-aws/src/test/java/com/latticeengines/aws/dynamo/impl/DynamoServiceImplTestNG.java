package com.latticeengines.aws.dynamo.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsResult;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesResult;
import com.amazonaws.services.applicationautoscaling.model.ScalableTarget;
import com.amazonaws.services.applicationautoscaling.model.ScalingPolicy;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.latticeengines.common.exposed.aws.DynamoOperation;


public class DynamoServiceImplTestNG extends DynamoFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DynamoServiceImplTestNG.class);

    @Value("${datacloud.match.entity.staging.table}")
    private String entityMatchTable;

    @BeforeClass(groups = "functional")
    private void setup() {
        tableName = this.getClass().getSimpleName() + "_" + env + "_" + stack;
        dynamoService.disableTableAutoScaling(tableName, DynamoOperation.Read);
        dynamoService.disableTableAutoScaling(tableName, DynamoOperation.Write);
        dynamoService.deleteTable(tableName);
        /*
         * To test for onDemand, set read and write capacity to 0 & disable
         * testAutoScaling as onDemand featured cannot be autoScaled
         */
        long readCapacityUnits = 10;
        long writeCapacityUnits = 10;
        String partitionKeyType = ScalarAttributeType.S.name();
        String sortKeyType = ScalarAttributeType.S.name();
        dynamoService.createTable(tableName, readCapacityUnits, writeCapacityUnits, PARTITION_KEY, partitionKeyType,
                SORT_KEY, sortKeyType, null);
    }

    @AfterClass(groups = "functional")
    private void teardown() {
        dynamoService.disableTableAutoScaling(tableName, DynamoOperation.Read);
        dynamoService.disableTableAutoScaling(tableName, DynamoOperation.Write);
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "functional")
    public void testDescribeTable() {
        TableDescription description = dynamoService.describeTable(entityMatchTable);
        Assert.assertNotNull(description);
        Assert.assertFalse(description.getKeySchema().isEmpty());
        description = dynamoService.describeTable(entityMatchTable);
        Assert.assertNotNull(description);
        Assert.assertFalse(description.getKeySchema().isEmpty());
        Assert.assertTrue(dynamoService.isCapacityOnDemand(entityMatchTable));
    }

    @Test(groups = "functional", dataProvider = "dynamoOperationProvider")
    public void testAutoScaling(DynamoOperation operation) {
        DescribeScalableTargetsResult target = dynamoService.describeScalableTargetsResult(tableName, operation);
        Assert.assertTrue(CollectionUtils.isEmpty(target.getScalableTargets()));
        DescribeScalingPoliciesResult policy = dynamoService.describeAutoScalingPolicy(tableName, operation);
        Assert.assertTrue(CollectionUtils.isEmpty(policy.getScalingPolicies()));

        dynamoService.enableTableAutoScaling(tableName, operation, 10, 50, 50);
        target = dynamoService.describeScalableTargetsResult(tableName, operation);
        Assert.assertTrue(CollectionUtils.isNotEmpty(target.getScalableTargets()));
        Assert.assertEquals(target.getScalableTargets().size(), 1);
        ScalableTarget tg = target.getScalableTargets().get(0);
        Assert.assertEquals(tg.getMinCapacity(), (Integer) 10);
        Assert.assertEquals(tg.getMaxCapacity(), (Integer) 50);
        policy = dynamoService.describeAutoScalingPolicy(tableName, operation);
        Assert.assertTrue(CollectionUtils.isNotEmpty(policy.getScalingPolicies()));
        Assert.assertEquals(policy.getScalingPolicies().size(), 1);
        ScalingPolicy sp = policy.getScalingPolicies().get(0);
        Assert.assertEquals(sp.getTargetTrackingScalingPolicyConfiguration().getTargetValue(), 50.0);

        dynamoService.disableTableAutoScaling(tableName, operation);
        target = dynamoService.describeScalableTargetsResult(tableName, operation);
        Assert.assertTrue(CollectionUtils.isEmpty(target.getScalableTargets()));
        policy = dynamoService.describeAutoScalingPolicy(tableName, operation);
        Assert.assertTrue(CollectionUtils.isEmpty(policy.getScalingPolicies()));
    }

    @DataProvider(name = "dynamoOperationProvider")
    private Object[][] dynamoOperationProvider() {
        return new Object[][] {
                { DynamoOperation.Read }, //
                { DynamoOperation.Write }, //
        };
}
}
