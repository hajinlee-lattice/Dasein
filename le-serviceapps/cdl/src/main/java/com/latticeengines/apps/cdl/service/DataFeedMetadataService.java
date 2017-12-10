package com.latticeengines.apps.cdl.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
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

    public abstract Table getMetadata(CDLImportConfig importConfig, String entity);

    public abstract Table resolveMetadata(Table original, Table schemaTable);

    public abstract boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType);

    public abstract CustomerSpace getCustomerSpace(CDLImportConfig importConfig);

    public abstract String getFileName(CDLImportConfig importConfig);

    public abstract String getFileDisplayName(CDLImportConfig importConfig);

    public abstract String getConnectorConfig(CDLImportConfig importConfig, String jobIdentifier);

    public boolean needUpdateDataFeedStatus() {
        return true;
    }

    public abstract Type getAvroType(Attribute attribute);

}
