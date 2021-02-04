package com.latticeengines.datafabric.service.datastore.impl;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;

@Component("dataService")
public class FabricDataServiceImpl implements FabricDataService {

    private static final Logger log = LoggerFactory.getLogger(FabricDataServiceImpl.class);

    Map<String, FabricDataServiceProvider> serviceProviders;

    @Inject
    DynamoDataServiceProvider dynamoService;

    public FabricDataServiceImpl() {
        log.debug("Initing fabric data service");
    }

    public FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema) {

        FabricDataStore dataStore = null;

        FabricDataServiceProvider dsp = getServiceProvider(store);
        if (dsp != null) {
            if (log.isDebugEnabled()) {
                log.debug("Initialize data store " + store + " repo " + repository);
            }
            dataStore = dsp.constructDataStore(repository, recordType, schema);
        } else {
            log.warn("Cannot find service provider for store " + store);
        }

        return dataStore;
    }

    public synchronized void addServiceProvider(FabricDataServiceProvider dsp) {
        if (serviceProviders == null)
            initServiceProviders();
        serviceProviders.put(dsp.getName(), dsp);
    }

    private synchronized FabricDataServiceProvider getServiceProvider(String store) {
        if (serviceProviders == null) {
            initServiceProviders();
        }
        return serviceProviders.get(store);
    }

    private void initServiceProviders() {

        serviceProviders = new HashMap<>();

        // Add preconfigured data services
        if (dynamoService != null) {
            serviceProviders.put(dynamoService.getName(), dynamoService);
        }
    }
}
