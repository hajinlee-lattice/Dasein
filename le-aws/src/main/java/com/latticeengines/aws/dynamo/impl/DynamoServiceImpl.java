package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingClient;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingClientBuilder;
import com.amazonaws.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsRequest;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsResult;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesRequest;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesResult;
import com.amazonaws.services.applicationautoscaling.model.MetricType;
import com.amazonaws.services.applicationautoscaling.model.ObjectNotFoundException;
import com.amazonaws.services.applicationautoscaling.model.PolicyType;
import com.amazonaws.services.applicationautoscaling.model.PredefinedMetricSpecification;
import com.amazonaws.services.applicationautoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.ScalableDimension;
import com.amazonaws.services.applicationautoscaling.model.ServiceNamespace;
import com.amazonaws.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.SSESpecification;
import com.amazonaws.services.dynamodbv2.model.SSEType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.aws.DynamoOperation;
import com.latticeengines.common.exposed.util.RetryUtils;

@Component("dynamoService")
public class DynamoServiceImpl implements DynamoService {

    private static final Logger log = LoggerFactory.getLogger(DynamoServiceImpl.class);

    private DynamoDB dynamoDB;
    private AmazonDynamoDB client;
    private AmazonDynamoDB remoteClient;
    private AmazonDynamoDB localClient;
    private AWSApplicationAutoScalingClient autoScaleClient;
    private String customerCMK;

    @Inject
    public DynamoServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.dynamo.endpoint}") String endpoint,
            @Value("${aws.region}") String region, @Value("${aws.dynamo.customer.cmk}") String customerCMK) {
        log.info("Constructing DynamoDB client using BasicAWSCredentials.");
        remoteClient = AmazonDynamoDBClientBuilder.standard() //
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                .withRegion(Regions.fromName(region)) //
                .build();
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Constructing DynamoDB client using endpoint " + endpoint);
            localClient = AmazonDynamoDBClientBuilder.standard() //
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)) //
                    .build();
        }
        client = remoteClient;
        dynamoDB = new DynamoDB(client);
        autoScaleClient = (AWSApplicationAutoScalingClient) AWSApplicationAutoScalingClientBuilder.standard() //
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                .withRegion(Regions.fromName(region)) //
                .build();
        this.customerCMK = customerCMK;
    }

    public DynamoServiceImpl(AmazonDynamoDB client) {
        this.client = client;
        this.remoteClient = client;
        this.localClient = client;
        this.dynamoDB = new DynamoDB(client);
    }

    @Override
    public AmazonDynamoDB getClient() {
        return client;
    }

    @Override
    public AmazonDynamoDB getRemoteClient() {
        return client;
    }

    @Override
    public DynamoDB getDynamo() {
        return this.dynamoDB;
    }

    @Override
    public void switchToLocal(boolean local) {
        if (local) {
            if (localClient != null) {
                this.client = localClient;
                log.info("Switch dynamo service to local mode.");
            } else {
                log.info("Local dynamo is not available in this environment.");
            }
        } else {
            this.client = remoteClient;
            log.info("Switch dynamo service to remote mode.");
        }
        this.dynamoDB = new DynamoDB(client);
    }

    @Override
    public Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType) {
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition key
        attributeDefinitions
                .add(new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

        if (sortKeyName != null) {
            keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort key
            attributeDefinitions
                    .add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType(sortKeyType));
        }
        SSESpecification sseSpecification = new SSESpecification().withEnabled(true);
        if (StringUtils.isNotBlank(customerCMK)) {
            sseSpecification.withKMSMasterKeyId(customerCMK).withSSEType(SSEType.KMS);
        }
        CreateTableRequest request = new CreateTableRequest() //
                .withTableName(tableName) //
                .withKeySchema(keySchema) //
                .withAttributeDefinitions(attributeDefinitions) //
                .withBillingMode(BillingMode.PAY_PER_REQUEST) //
                .withSSESpecification(sseSpecification);

        try {
            log.info("Creating table " + tableName);
            TableUtils.createTableIfNotExists(client, request);
            Table table = dynamoDB.getTable(tableName);
            log.info("Waiting for table " + tableName + " to become active. This may take a while.");
            waitForTableActivated(table);
            return table;
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to create table " + tableName, e);
        }
    }

    // waitForActive AWS API sometimes doesn't wait for long enough time until
    // table to be active
    private void waitForTableActivated(Table table) throws InterruptedException {
        int retries = 0;
        while (retries++ < 2) {
            try {
                table.waitForActive();
                return;
            } catch (InterruptedException e) {
                log.warn("Wait interrupted.", e);
            }
        }
        table.waitForActive();
    }

    @Override
    public void deleteTable(String tableName) {
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        try {
            TableUtils.deleteTableIfExists(client, request);
            while (hasTable(tableName)) {
                log.info("Wait 1 sec for deleting " + tableName);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete dynamo table " + tableName, e);
        }
    }

    @Override
    public boolean hasTable(String tableName) {
        return client.listTables().getTableNames().contains(tableName);
    }

    @Override
    public void updateTableThroughput(String tableName, long readCapacity, long writeCapacity) {
        Table table = dynamoDB.getTable(tableName);
        log.info(String.format("Modifying provisioned throughput for %s to read=%d and write=%d", tableName, readCapacity,
                writeCapacity));
        try {
            table.updateTable(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
            table.waitForActive();
        } catch (Exception e) {
            throw new RuntimeException("UpdateTable request failed for " + tableName, e);
        }
    }

    @Override
    public void tagTable(String tableName, Map<String, String> tags) {
        String tableArn = client.describeTable(tableName).getTable().getTableArn();
        List<Tag> dynamoTags = new ArrayList<>();
        tags.forEach((k, v) -> {
            Tag dynamoTag = new Tag().withKey(k).withValue(v);
            dynamoTags.add(dynamoTag);
        });
        TagResourceRequest request = new TagResourceRequest();
        request.setTags(dynamoTags);
        request.setResourceArn(tableArn);
        TagResourceResult result = client.tagResource(request);
        log.info("TagResourceResult: " + result.toString());
    }

    @Override
    public TableDescription describeTable(String tableName) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        DescribeTableResult result = retry.execute(ctx -> getClient().describeTable(tableName));
        if (result != null) {
            return result.getTable();
        } else {
            return null;
        }
    }

    @Override
    public void enableTableAutoScaling(String tableName, DynamoOperation operation, int minCapacityUnits,
            int maxCapacityUnits, double utilizedTargetPct) {
        ServiceNamespace ns = ServiceNamespace.Dynamodb;
        ScalableDimension dimension = operation == DynamoOperation.Read
                ? ScalableDimension.DynamodbTableReadCapacityUnits
                : ScalableDimension.DynamodbTableWriteCapacityUnits;
        String resourceID = buildTableResourceID(tableName);
        String tableArn = client.describeTable(tableName).getTable().getTableArn();
        String policyName = buildScalingPolicyName(tableName);

        // Define scalable target
        RegisterScalableTargetRequest rstRequest = new RegisterScalableTargetRequest() //
                .withServiceNamespace(ns) //
                .withResourceId(resourceID) //
                .withScalableDimension(dimension) //
                .withMinCapacity(minCapacityUnits) //
                .withMaxCapacity(maxCapacityUnits) //
                .withRoleARN(tableArn);
        try {
            autoScaleClient.registerScalableTarget(rstRequest);
        } catch (Exception e) {
            throw new RuntimeException("Unable to enable autoscaling on table " + tableName, e);
        }

        // Configure a scaling policy
        TargetTrackingScalingPolicyConfiguration scalingPolicyConfig = new TargetTrackingScalingPolicyConfiguration() //
                .withPredefinedMetricSpecification(new PredefinedMetricSpecification() //
                        .withPredefinedMetricType(
                                operation == DynamoOperation.Read ? MetricType.DynamoDBReadCapacityUtilization
                                        : MetricType.DynamoDBWriteCapacityUtilization)) //
                .withTargetValue(utilizedTargetPct);

        // Create the scaling policy, based on your configuration
        PutScalingPolicyRequest pspRequest = new PutScalingPolicyRequest() //
                .withServiceNamespace(ns) //
                .withScalableDimension(dimension) //
                .withResourceId(resourceID) //
                .withPolicyName(policyName) //
                .withPolicyType(PolicyType.TargetTrackingScaling) //
                .withTargetTrackingScalingPolicyConfiguration(scalingPolicyConfig);

        try {
            autoScaleClient.putScalingPolicy(pspRequest);
        } catch (Exception e) {
            throw new RuntimeException("Unable to configure autoscaling on table " + tableName, e);
        }
    }

    @Override
    public void disableTableAutoScaling(String tableName, DynamoOperation operation) {
        ServiceNamespace ns = ServiceNamespace.Dynamodb;
        ScalableDimension dimension = operation == DynamoOperation.Read
                ? ScalableDimension.DynamodbTableReadCapacityUnits
                : ScalableDimension.DynamodbTableWriteCapacityUnits;
        String resourceID = buildTableResourceID(tableName);
        String policyName = buildScalingPolicyName(tableName);

        // Delete the scaling policy
        DeleteScalingPolicyRequest delSPRequest = new DeleteScalingPolicyRequest() //
                .withServiceNamespace(ns) //
                .withScalableDimension(dimension) //
                .withResourceId(resourceID) //
                .withPolicyName(policyName);

        try {
            autoScaleClient.deleteScalingPolicy(delSPRequest);
        } catch (ObjectNotFoundException e) {
            log.warn("Scaling policy doesn't exist on table " + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Unable to delete scaling policy for table " + tableName, e);
        }

        // Delete the scalable target
        DeregisterScalableTargetRequest delSTRequest = new DeregisterScalableTargetRequest() //
                .withServiceNamespace(ns) //
                .withScalableDimension(dimension) //
                .withResourceId(resourceID);

        try {
            autoScaleClient.deregisterScalableTarget(delSTRequest);
        } catch (ObjectNotFoundException e) {
            log.warn("Scalable target doesn't exist on table " + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Unable to delete scalable target on table " + tableName, e);
        }
    }

    @Override
    public DescribeScalableTargetsResult describeScalableTargetsResult(String tableName, DynamoOperation operation) {
        ServiceNamespace ns = ServiceNamespace.Dynamodb;
        ScalableDimension dimension = operation == DynamoOperation.Read
                ? ScalableDimension.DynamodbTableReadCapacityUnits
                : ScalableDimension.DynamodbTableWriteCapacityUnits;
        String resourceID = buildTableResourceID(tableName);

        DescribeScalableTargetsRequest dscRequest = new DescribeScalableTargetsRequest() //
                .withServiceNamespace(ns) //
                .withScalableDimension(dimension) //
                .withResourceIds(resourceID);

        try {
            return autoScaleClient.describeScalableTargets(dscRequest);
        } catch (Exception e) {
            throw new RuntimeException("Unable to describe scalable target on table " + tableName, e);
        }
    }

    @Override
    public DescribeScalingPoliciesResult describeAutoScalingPolicy(String tableName, DynamoOperation operation) {
        ServiceNamespace ns = ServiceNamespace.Dynamodb;
        ScalableDimension dimension = operation == DynamoOperation.Read
                ? ScalableDimension.DynamodbTableReadCapacityUnits
                : ScalableDimension.DynamodbTableWriteCapacityUnits;
        String resourceID = buildTableResourceID(tableName);

        DescribeScalingPoliciesRequest dspRequest = new DescribeScalingPoliciesRequest().withServiceNamespace(ns)
                .withScalableDimension(dimension).withResourceId(resourceID);

        try {
            DescribeScalingPoliciesResult dspResult = autoScaleClient.describeScalingPolicies(dspRequest);
            return dspResult;
        } catch (Exception e) {
            throw new RuntimeException("Unable to describe autoscaling policy on table " + tableName, e);
        }
    }

    @Override
    public boolean isCapacityOnDemand(String tableName) {
        TableDescription description = describeTable(tableName);
        if (description == null || description.getBillingModeSummary() == null) {
            return false;
        }
        return "PAY_PER_REQUEST".equals(description.getBillingModeSummary().getBillingMode());
    }

    private static String buildTableResourceID(String tableName) {
        return "table/" + tableName;
    }

    private static String buildScalingPolicyName(String tableName) {
        return "ScalingPolicy_" + tableName;
    }

}
