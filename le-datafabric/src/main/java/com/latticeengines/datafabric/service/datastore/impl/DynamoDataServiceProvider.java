package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.dynamo.DynamoService;
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

    @Autowired
    private DynamoService dynamoService;

    public DynamoDataServiceProvider() {
    }

    public DynamoDataServiceProvider(String endPoint) {
        this.endPoint = endPoint;
    }

    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {
        log.info("Initialize dynamo data store " + " repo " + repository + " record " + recordType);
        return new DynamoDataStoreImpl(dynamoService.getClient(), repository, recordType, schema);
    }

    public String getName() {
        return name;
    }
}
