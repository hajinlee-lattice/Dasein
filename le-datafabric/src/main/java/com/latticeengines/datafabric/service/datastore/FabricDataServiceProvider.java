package com.latticeengines.datafabric.service.datastore;

import org.apache.avro.Schema;

public interface FabricDataServiceProvider {

    FabricDataStore constructDataStore(String repository, String recordType, Schema schema);

    String getName();

}

