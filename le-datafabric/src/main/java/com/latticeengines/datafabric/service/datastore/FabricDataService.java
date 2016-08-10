package com.latticeengines.datafabric.service.datastore;

import org.apache.avro.Schema;

public interface FabricDataService {

    FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema);
    void addServiceProvider(FabricDataServiceProvider dsp); 
}

