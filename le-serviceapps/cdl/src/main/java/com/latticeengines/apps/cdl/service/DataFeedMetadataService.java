package com.latticeengines.apps.cdl.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public abstract class DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedMetadataService.class);

    protected static final String USER_PREFIX = "user_";

    private static Map<String, DataFeedMetadataService> services = new HashMap<>();

    protected DataFeedMetadataService(String serviceName) {
        services.put(serviceName, this);
    }

    public static DataFeedMetadataService getService(String serviceName) {
        return services.get(serviceName);
    }

    public abstract Pair<Table, List<AttrConfig>> getMetadata(CDLImportConfig importConfig, String entity);

    public abstract Table resolveMetadata(Table original, Table schemaTable);

    public abstract boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType);

    public abstract CustomerSpace getCustomerSpace(CDLImportConfig importConfig);

    public abstract CSVImportFileInfo getImportFileInfo(CDLImportConfig importConfig);

    public abstract String getConnectorConfig(CDLImportConfig importConfig, String jobIdentifier);

    public boolean needUpdateDataFeedStatus() {
        return true;
    }

    public abstract Type getAvroType(Attribute attribute);

    public abstract void autoSetCDLExternalSystem(CDLExternalSystemService cdlExternalSystemService, Table table,
            String customerSpace);

    public boolean validateOriginalTable(Table original) {
        // 1. Table from source cannot be null.
        if (original == null) {
            return false;
        }
        return true;
    }

    public void applyAttributePrefix(Table importTable, Table templateTable) {
        if (importTable == null || CollectionUtils.isEmpty(importTable.getAttributes())) {
            throw new IllegalArgumentException("Import table cannot be null or empty!");
        }
        if (templateTable == null || CollectionUtils.isEmpty(templateTable.getAttributes())) {
            throw new IllegalArgumentException("Template table cannot be null or empty!");
        }
        Set<String> templateAttrs = templateTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());
        importTable.getAttributes().forEach(attribute -> {
            if (!templateAttrs.contains(attribute.getName())) {
                log.info(String.format("Reset attribute name %s -> %s", attribute.getName(),
                        USER_PREFIX + attribute.getName()));
                attribute.setName(USER_PREFIX + attribute.getName());
            }
        });
    }

}
