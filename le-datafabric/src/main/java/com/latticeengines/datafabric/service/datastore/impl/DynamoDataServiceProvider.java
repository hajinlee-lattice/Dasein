package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;

@Component("dynamoDataService")
public class DynamoDataServiceProvider implements FabricDataServiceProvider {

    private static final Logger log = LoggerFactory.getLogger(FabricDataServiceImpl.class);

    @Autowired
    private DynamoService dynamoService;

    @Override
    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {
        log.info("Initialize dynamo data store " + " repo " + repository + " record " + recordType);
        return new DynamoDataStoreImpl(dynamoService, repository, recordType, schema);
    }

    @Override
    public String getName() {
        return FabricStoreEnum.DYNAMO.name();
    }

    public void setDynamoService(DynamoService dynamoService) {
        this.dynamoService = dynamoService;
    }

}
