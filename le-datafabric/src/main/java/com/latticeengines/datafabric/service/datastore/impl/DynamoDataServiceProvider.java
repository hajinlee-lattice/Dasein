package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

@Component("dynamoDataService")
public class DynamoDataServiceProvider implements FabricDataServiceProvider {

    private static final Log log = LogFactory.getLog(FabricDataServiceImpl.class);

    private final String name = "DYNAMO";

    @Value("${datafabric.dataService.dynamo.endpoint:http://localhost:8000}")
    private String endPoint;


    @Value("${datafabric.dataService.dynamo.accessKey:hup2R9WP51zD3YXRxv6nj5ErjuYArOjTxjn15nmQ4dM=")
    private String accessKey;

    @Value("${datafabric.dataService.dynamo.secretKey:JBblsfXyUAzeuY5OtrbeaOqR9wj5IFkhFWE+vfSr/nWpCPkbZc2xWUzXOUR2YdYn")
    private String secretKey;

    private AmazonDynamoDBClient client = null;
    private boolean initialized = false;

    public DynamoDataServiceProvider() {
    }


    public DynamoDataServiceProvider(String endPoint) {
        this.endPoint = endPoint;
    }

    synchronized private void init() {
        if (initialized) return;

        log.info("Initialize dynamo data service with endPoint " + endPoint);
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        client = new AmazonDynamoDBClient(credentials).withEndpoint(endPoint);

        initialized = true;
    }

    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {

        FabricDataStore dataStore  = null;
        log.info("Initialize dynamo data store " + " repo " + repository + " record " + recordType);
        if (!initialized) init();

        dataStore = new DynamoDataStoreImpl(client, repository, recordType, schema);
        return dataStore;
    }

    public String getName() {
         return name;
    }
}
