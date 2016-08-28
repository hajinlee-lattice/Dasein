package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
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

    @Value("${aws.dynamo.endpoint:}")
    private String endPoint;

    @Value("${aws.default.access.key.encrypted}")
    private String accessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String secretKey;

    private AmazonDynamoDBClient client = null;
    private boolean initialized = false;

    public DynamoDataServiceProvider() {
    }

    public DynamoDataServiceProvider(String endPoint) {
        this.endPoint = endPoint;
    }

    synchronized private void init() {
        if (initialized) {
            return;
        }

        if (StringUtils.isNotEmpty(endPoint)) {
            log.info("Initialize dynamo data service with endPoint " + endPoint);
            client = new AmazonDynamoDBClient().withEndpoint(endPoint);
        } else {
            log.info("Initialize dynamo data service with BasicAWSCredentials");
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            client = new AmazonDynamoDBClient(credentials);
        }

        initialized = true;
    }

    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {

        FabricDataStore dataStore = null;
        log.info("Initialize dynamo data store " + " repo " + repository + " record " + recordType);
        if (!initialized)
            init();

        dataStore = new DynamoDataStoreImpl(client, repository, recordType, schema);
        return dataStore;
    }

    public String getName() {
        return name;
    }
}
