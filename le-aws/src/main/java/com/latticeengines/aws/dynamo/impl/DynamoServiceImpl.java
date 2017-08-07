package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.latticeengines.aws.dynamo.DynamoService;

@Component("dynamoService")
public class DynamoServiceImpl implements DynamoService {

    private static final Logger log = LoggerFactory.getLogger(DynamoServiceImpl.class);

    private DynamoDB dynamoDB;
    private AmazonDynamoDB client;
    private AmazonDynamoDB remoteClient;

    @Autowired
    public DynamoServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.dynamo.endpoint}") String endpoint,
            @Value("${aws.region}") String region) {
        log.info("Constructing DynamoDB client using BasicAWSCredentials.");
        remoteClient = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                .withRegion(Regions.fromName(region)) //
                .build();
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Constructing DynamoDB client using endpoint " + endpoint);
            client = AmazonDynamoDBClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)) //
                    .build();
            dynamoDB = new DynamoDB(client);
        } else {
            client = remoteClient;
            dynamoDB = new DynamoDB(remoteClient);
        }
    }

    public DynamoServiceImpl(AmazonDynamoDB client) {
        this.client = client;
        this.dynamoDB = new DynamoDB(client);
    }

    @Override
    public AmazonDynamoDB getClient() {
        return client;
    }

    @Override
    public AmazonDynamoDB getRemoteClient() {
        return remoteClient;
    }

    @Override
    public Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
            String partitionKeyType, String sortKeyName, String sortKeyType) {
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
                                                                                                             // key
        attributeDefinitions
                .add(new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

        if (sortKeyName != null) {
            keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort
                                                                                                             // key
            attributeDefinitions
                    .add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType(sortKeyType));
        }

        CreateTableRequest request = new CreateTableRequest().withTableName(tableName).withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions).withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));

        try {
            log.info("Creating table " + tableName);
            TableUtils.createTableIfNotExists(client, request);
            Table table = dynamoDB.getTable(tableName);
            log.info("Waiting for table " + tableName + " to become active. This may take a while.");
            table.waitForActive();
            return table;
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to create table " + tableName, e);
        }
    }

    @Override
    public void deleteTable(String tableName) {
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        try {
            TableUtils.deleteTableIfExists(client, request);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete dynamo table " + tableName, e);
        }
    }
}
