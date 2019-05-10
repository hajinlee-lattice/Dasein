package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.SSESpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.util.RetryUtils;

@Component("dynamoService")
public class DynamoServiceImpl implements DynamoService {

    private static final Logger log = LoggerFactory.getLogger(DynamoServiceImpl.class);

    private DynamoDB dynamoDB;
    private AmazonDynamoDB client;
    private AmazonDynamoDB remoteClient;
    private AmazonDynamoDB localClient;

    @Autowired
    public DynamoServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.dynamo.endpoint}") String endpoint,
            @Value("${aws.region}") String region) {
        log.info("Constructing DynamoDB client using BasicAWSCredentials.");
        remoteClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                .withRegion(Regions.fromName(region)) //
                .build();
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Constructing DynamoDB client using endpoint " + endpoint);
            localClient = AmazonDynamoDBClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)) //
                    .build();
        }
        client = remoteClient;
        dynamoDB = new DynamoDB(client);
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

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits);
        CreateTableRequest request = new CreateTableRequest() //
                .withTableName(tableName) //
                .withKeySchema(keySchema) //
                .withAttributeDefinitions(attributeDefinitions) //
                .withProvisionedThroughput(provisionedThroughput) //
                .withSSESpecification(new SSESpecification().withEnabled(true));

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

}
