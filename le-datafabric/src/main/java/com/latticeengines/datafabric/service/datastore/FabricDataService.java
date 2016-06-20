package com.latticeengines.datafabric.service.datastore;

import org.apache.avro.Schema;

public interface FabricDataService {

    void init();

    FabricDataStore constructDataStore(String store, String repository, String recordType, Schema schema);
}

