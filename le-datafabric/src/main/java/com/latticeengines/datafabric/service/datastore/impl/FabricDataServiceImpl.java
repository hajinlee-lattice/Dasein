package com.latticeengines.datafabric.service.datastore.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

@Component("dataService")
public class FabricDataServiceImpl implements FabricDataService {

    private static final Log log = LogFactory.getLog(FabricDataServiceImpl.class);

    Map<String, FabricDataServiceProvider> serviceProviders;

    @Autowired
    DynamoDataServiceProvider dynamoService;
    @Autowired
    RedisDataServiceProvider redisService;

    public FabricDataServiceImpl() {
        log.info("Initing fabric data service");
    }

    public FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema) {

        FabricDataStore dataStore = null;

        if (serviceProviders == null)
            initServiceProviders();
        FabricDataServiceProvider dsp = serviceProviders.get(store);
        ;
        if (dsp != null) {
            log.info("Initialize data store " + store + " repo " + repository);
            dataStore = dsp.constructDataStore(repository, recordType, schema);
        }

        return dataStore;
    }

    synchronized public void addServiceProvider(FabricDataServiceProvider dsp) {
        if (serviceProviders == null)
            initServiceProviders();
        serviceProviders.put(dsp.getName(), dsp);
    }

    synchronized private void initServiceProviders() {
        serviceProviders = new HashMap<String, FabricDataServiceProvider>();

        // Add preconfigured data services
        if (redisService != null) {
            serviceProviders.put(redisService.getName(), redisService);
        }
        if (dynamoService != null) {
            serviceProviders.put(dynamoService.getName(), dynamoService);
        }
    }
}
