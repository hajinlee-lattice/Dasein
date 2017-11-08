package com.latticeengines.apps.cdl.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class DataFeedMetadataService {

    private static Map<String, DataFeedMetadataService> services = new HashMap<>();

    protected DataFeedMetadataService(String serviceName) {
        services.put(serviceName, this);
    }

    public static DataFeedMetadataService getService(String serviceName) {
        return services.get(serviceName);
    }

    public abstract Table getMetadata(String metadataStr);

    public abstract Table resolveMetadata(Table original, Table schemaTable);

    public abstract boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType);

    public abstract CustomerSpace getCustomerSpace(String metadataStr);

    public abstract String getFileName(String metadataStr);

    public abstract String getFileDisplayName(String metadataStr);

    public abstract String getConnectorConfig(String metadataStr, String jobIdentifier);

    public boolean needUpdateDataFeedStatus() {
        return false;
    }

    public abstract Type getAvroType(Attribute attribute);

}
