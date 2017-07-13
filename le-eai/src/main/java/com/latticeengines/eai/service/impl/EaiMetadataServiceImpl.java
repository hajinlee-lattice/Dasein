package com.latticeengines.eai.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.method.P;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("eaiMetadataService")
public class EaiMetadataServiceImpl implements EaiMetadataService {

    private static final Logger log = LoggerFactory.getLogger(EaiMetadataServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void registerTables(List<Table> tablesMetadataFromImport, ImportContext importContext) {
        String customer = importContext.getProperty(ImportProperty.CUSTOMER, String.class);
        String customerSpace = CustomerSpace.parse(customer).toString();
        updateTables(customerSpace, tablesMetadataFromImport);
    }

    @Override
    public List<Table> getImportTables(String customerSpace) {
        return metadataProxy.getImportTables(customerSpace);
    }

    @Override
    public void updateTables(String customerSpace, List<Table> tables) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);

        for (Table table : tables) {
            metadataProxy.updateTable(customerSpace, table.getName(), table);
        }

    }

    @Override
    public LastModifiedKey getLastModifiedKey(String customerSpace, Table table) {
        Table newTable = metadataProxy.getTable(customerSpace, table.getName());
        if (newTable != null) {
            return newTable.getLastModifiedKey();
        }
        newTable = metadataProxy.getImportTable(customerSpace, table.getName());
        if (newTable != null) {
            return newTable.getLastModifiedKey();
        }
        throw new LedpException(LedpCode.LEDP_17007, new String[] { table.getName(), customerSpace });
    }

    private void addTenantToTable(Table table, String customerSpace) {
        Tenant tenant = getTenant(customerSpace);
        table.setTenant(tenant);

        List<Attribute> attributes = table.getAttributes();
        for (Attribute attribute : attributes) {
            log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
        }

    }

    @VisibleForTesting
    void addExtractToTable(Table table, String path, long processedRecords) {
        Extract e = createExtract(path, processedRecords);
        table.addExtract(e);
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    private Tenant getTenant(String customerSpace) {
        return tenantService.findByTenantId(customerSpace);
    }

    @Override
    public void updateTableSchema(List<Table> tablesMetadataFromImport, ImportContext importContext) {
        String customer = importContext.getProperty(ImportProperty.CUSTOMER, String.class);
        String customerSpace = CustomerSpace.parse(customer).toString();
        @SuppressWarnings("unchecked")
        Map<String, String> targetPathsMap = importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = importContext.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);

        for (Table table : tablesMetadataFromImport) {
            addTenantToTable(table, customerSpace);
            addExtractToTable(table, targetPathsMap.get(table.getName()), processedRecordsMap.get(table.getName()));
            setLastModifiedTimeStamp(table, importContext);
        }
    }

    @Override
    public Table getTable(String customerSpace, String tableName) {
        return metadataProxy.getTable(customerSpace, tableName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, List<Extract>> getExtractsForTable(List<Table> tableMetaData, ImportContext importContext) {
        Map<String, Boolean> mutipleExtractMap = importContext.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class);
        Map<String, String> targetPathsMap = importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class);
        Map<String, Long> processedRecordsMap = importContext.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        Map<String, List<String>> multipleTargets = importContext.getProperty(ImportProperty.EXTRACT_PATH_LIST,
                Map.class);
        Map<String, List<Long>> multipleRecords = importContext.getProperty(ImportProperty.EXTRACT_RECORDS_LIST,
                Map.class);

        HashMap<String, List<Extract>> extracts = new HashMap<>();
        for (Table table : tableMetaData) {
            extracts.put(table.getName(), new ArrayList<Extract>());
            if (mutipleExtractMap.get(table.getName())) {
                for (int i = 0; i < multipleTargets.get(table.getName()).size(); i++) {
                    extracts.get(table.getName()).add(createExtract(multipleTargets.get(table.getName()).get(i),
                            multipleRecords.get(table.getName()).get(i)));
                }
            } else {
                extracts.get(table.getName()).add(createExtract(targetPathsMap.get(table.getName()),
                        processedRecordsMap.get(table.getName())));
            }
        }


//        HashMap<String, Extract> extracts = new HashMap<>();
//        for (Table table : tableMetaData) {
//            extracts.put(table.getName(),
//                    createExtract(targetPathsMap.get(table.getName()), processedRecordsMap.get(table.getName())));
//        }
        return extracts;
    }


    @VisibleForTesting
    void setLastModifiedTimeStamp(Table table, ImportContext importContext) {
        @SuppressWarnings("unchecked")
        Map<String, Long> map = importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        LastModifiedKey lmk = table.getLastModifiedKey();
        if (lmk != null) {
            Long lastModifiedDateValue = map.get(table.getName());
            lmk.setLastModifiedTimestamp(lastModifiedDateValue);
            log.info(
                    "After import Table: " + table.getName() + " has LastModifedKeyTimeStamp:" + lastModifiedDateValue);
        }
    }

    @VisibleForTesting
    void setMetadatProxy(MetadataProxy metadataProxy) {
        this.metadataProxy = metadataProxy;
    }

    @VisibleForTesting
    void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
