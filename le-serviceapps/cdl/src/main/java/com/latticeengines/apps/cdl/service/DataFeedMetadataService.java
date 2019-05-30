package com.latticeengines.apps.cdl.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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

    public void applyAttributePrefix(CDLExternalSystemService cdlExternalSystemService, String customerSpace,
            Table importTable, Table templateTable, Table existingTemplate) {
        if (cdlExternalSystemService == null) {
            throw new IllegalArgumentException("ExternalSystemService cannot be null when set custom prefix!");
        }
        if (importTable == null || CollectionUtils.isEmpty(importTable.getAttributes())) {
            throw new IllegalArgumentException("Import table cannot be null or empty!");
        }
        if (templateTable == null || CollectionUtils.isEmpty(templateTable.getAttributes())) {
            throw new IllegalArgumentException("Template table cannot be null or empty!");
        }
        Set<String> templateAttrs = templateTable.getAttributes().stream().map(Attribute::getName)
                .collect(Collectors.toSet());
        Set<String> existingAttrs = existingTemplate == null ? new HashSet<>() :
                existingTemplate.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());
        Map<String, String> nameMap = new HashMap<>();
        importTable.getAttributes().forEach(attribute -> {
            if (!templateAttrs.contains(attribute.getName())
                    && !attribute.getName().startsWith(USER_PREFIX)
                    && !existingAttrs.contains(attribute.getName())) {
                String originalName = attribute.getName();
                String userAttrName = USER_PREFIX + attribute.getName();
                log.info(String.format("Reset attribute name %s -> %s", attribute.getName(), userAttrName));
                attribute.setName(userAttrName);
                nameMap.put(originalName, userAttrName);
            }
        });
        BusinessEntity[] entities = {BusinessEntity.Account, BusinessEntity.Contact};
        for (BusinessEntity entity : entities) {
            CDLExternalSystem cdlExternalSystem = cdlExternalSystemService.getExternalSystem(customerSpace,
                    entity);
            if (cdlExternalSystem != null && (CollectionUtils.isNotEmpty(cdlExternalSystem.getCRMIdList())
                    || CollectionUtils.isNotEmpty(cdlExternalSystem.getERPIdList())
                    || CollectionUtils.isNotEmpty(cdlExternalSystem.getMAPIdList())
                    || CollectionUtils.isNotEmpty(cdlExternalSystem.getOtherIdList()))) {
                if (CollectionUtils.isNotEmpty(cdlExternalSystem.getCRMIdList())) {
                    List<String> newCRMIdList = new ArrayList<>();
                    cdlExternalSystem.getCRMIdList().forEach(id -> {
                        newCRMIdList.add(nameMap.getOrDefault(id, id));
                    });
                    cdlExternalSystem.setCRMIdList(newCRMIdList);
                }
                if (CollectionUtils.isNotEmpty(cdlExternalSystem.getERPIdList())) {
                    List<String> newERPIdList = new ArrayList<>();
                    cdlExternalSystem.getERPIdList().forEach(id -> {
                        newERPIdList.add(nameMap.getOrDefault(id, id));
                    });
                    cdlExternalSystem.setERPIdList(newERPIdList);
                }
                if (CollectionUtils.isNotEmpty(cdlExternalSystem.getMAPIdList())) {
                    List<String> newMAPIdList = new ArrayList<>();
                    cdlExternalSystem.getMAPIdList().forEach(id -> {
                        newMAPIdList.add(nameMap.getOrDefault(id, id));
                    });
                    cdlExternalSystem.setMAPIdList(newMAPIdList);
                }
                if (CollectionUtils.isNotEmpty(cdlExternalSystem.getOtherIdList())) {
                    List<String> newOtherIdList = new ArrayList<>();
                    cdlExternalSystem.getOtherIdList().forEach(id -> {
                        newOtherIdList.add(nameMap.getOrDefault(id, id));
                    });
                    cdlExternalSystem.setOtherIdList(newOtherIdList);
                }
                List<Pair<String, String>> idMappings = cdlExternalSystem.getIdMappingList();
                // This set should not be null
                // There's some backward compatible issue, will delete the error
                // records later
                if (CollectionUtils.isNotEmpty(idMappings)) {
                    List<Pair<String, String>> userIdMapping = new ArrayList<>();
                    for (Pair<String, String> idMapping : idMappings) {
                        if (nameMap.containsKey(idMapping.getLeft())) {
                            userIdMapping.add(Pair.of(nameMap.get(idMapping.getLeft()), idMapping.getRight()));
                        } else {
                            userIdMapping.add(idMapping);
                        }
                    }
                    cdlExternalSystem.setIdMapping(userIdMapping);
                    cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem,
                            entity);
                }
            }
        }
    }

}
