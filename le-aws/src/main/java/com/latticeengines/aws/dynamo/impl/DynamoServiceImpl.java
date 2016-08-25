package com.latticeengines.aws.dynamo.impl;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log log = LogFactory.getLog(DynamoServiceImpl.class);

    private AmazonDynamoDBClient client;
    private DynamoDB dynamoDB;

    @Autowired
    public DynamoServiceImpl(BasicAWSCredentials awsCredentials,
                             @Value("${aws.dynamo.endpoint:}") String endpoint) {
        if (StringUtils.isEmpty(endpoint)) {
            log.info("Constructing DynamoServiceImpl using BasicAWSCredentials.");
            client = new AmazonDynamoDBClient(awsCredentials);
        } else {
            log.info("Constructing DynamoServiceImpl using endpoint " + endpoint);
            client = new AmazonDynamoDBClient().withEndpoint(endpoint);
        }
        dynamoDB = new DynamoDB(client);
    }

    public DynamoServiceImpl(AmazonDynamoDBClient client) {
        this.client = client;
        this.dynamoDB = new DynamoDB(client);
    }

    public Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
                            String partitionKeyName, String partitionKeyType,
                            String sortKeyName, String sortKeyType) {
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        keySchema.add(new KeySchemaElement()
                .withAttributeName(partitionKeyName)
                .withKeyType(KeyType.HASH)); //Partition key
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(partitionKeyName)
                .withAttributeType(partitionKeyType));

        if (sortKeyName != null) {
            keySchema.add(new KeySchemaElement()
                    .withAttributeName(sortKeyName)
                    .withKeyType(KeyType.RANGE)); //Sort key
            attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(sortKeyName)
                    .withAttributeType(sortKeyType));
        }

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput( new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));

        try {
            log.info("Creating table " + tableName);
            Table table = dynamoDB.createTable(request);
            table.waitForActive();
            return table;
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to create table " + tableName, e);
        }
    }

    public void deleteTable(String tableName) {
        if (client.listTables().getTableNames().contains(tableName)) {
            client.deleteTable(tableName);
            long startTime = System.currentTimeMillis();
            while (client.listTables().getTableNames().contains(tableName)) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    // ignore
                }
                if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(30)) {
                    throw new RuntimeException("Waited 30 min for deleting table " + tableName);
                }
            }
        }
    }
}
