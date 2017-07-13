package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.latticeengines.aws.dynamo.DynamoService;

@Component("dynamoService")
public class DynamoServiceImpl implements DynamoService {

    private static final Logger log = LoggerFactory.getLogger(DynamoServiceImpl.class);

    private AmazonDynamoDBClient client;
    private AmazonDynamoDBClient remoteClient;

    private DynamoDB dynamoDB;

    @Autowired
    public DynamoServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.dynamo.endpoint}") String endpoint) {
        log.info("Constructing DynamoDB client using BasicAWSCredentials.");
        remoteClient = new AmazonDynamoDBClient(awsCredentials);
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Constructing DynamoDB client using endpoint " + endpoint);
            client = new AmazonDynamoDBClient(awsCredentials).withEndpoint(endpoint);
        } else {
            client = new AmazonDynamoDBClient(awsCredentials);
        }
        dynamoDB = new DynamoDB(client);
    }

    public DynamoServiceImpl(AmazonDynamoDBClient client) {
        this.client = client;
        this.dynamoDB = new DynamoDB(client);
    }

    @Override
    public AmazonDynamoDBClient getClient() {
        return client;
    }

    @Override
    public AmazonDynamoDBClient getRemoteClient() {
        return remoteClient;
    }

    @Override
    public DynamoDB getDynamoDB() {
        return dynamoDB;
    }

    @Override
    public Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
            String partitionKeyName, String partitionKeyType, String sortKeyName, String sortKeyType) {
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
                                                                                                             // key
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(
                partitionKeyType));

        if (sortKeyName != null) {
            keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort
                                                                                                             // key
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType(
                    sortKeyType));
        }

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(
                        new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(
                                writeCapacityUnits));

        try {
            log.info("Creating table " + tableName);
            Table table = dynamoDB.createTable(request);
            log.info("Waiting for table " + tableName + " to become active. This may take a while.");
            table.waitForActive();
            return table;
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to create table " + tableName, e);
        }
    }

    @Override
    public void deleteTable(String tableName) {
        if (client.listTables().getTableNames().contains(tableName)) {
            client.deleteTable(tableName);
            try {
                log.info("Waiting for " + tableName + " to be deleted...this may take a while...");
                long startTime = System.currentTimeMillis();
                while (client.listTables().getTableNames().contains(tableName)) {
                    if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(30)) {
                        throw new RuntimeException("Failed to delete table within 30 min");
                    }
                    Thread.sleep(3000L);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete dynamo table " + tableName, e);
            }
        }
    }
}
