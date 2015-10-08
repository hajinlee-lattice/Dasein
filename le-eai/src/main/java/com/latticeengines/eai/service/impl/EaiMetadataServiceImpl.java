package com.latticeengines.eai.service.impl;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.security.exposed.service.TenantService;

@Component("eaiMetadataService")
public class EaiMetadataServiceImpl implements EaiMetadataService {

    private static final Log log = LogFactory.getLog(EaiMetadataServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Override
    public void registerTables(List<Table> tables, ImportContext importContext) {
        String customer = importContext.getProperty(ImportProperty.CUSTOMER, String.class);
        @SuppressWarnings("unchecked")
        Map<String, String> targetPaths = importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        for (Table table : tables) {
            addTenantToTable(table, customer);
            addExtractToTable(table, targetPaths.get(table.getName()));
            PrimaryKey pk = createPrimaryKey();
            LastModifiedKey lk = createLastModifiedKey();
            for (Attribute attr : table.getAttributes()) {
                if (attr.getLogicalDataType().equals("id")) {
                    pk.addAttribute(attr.getName());
                } else if (attr.getName().equals("LastModifiedDate")) {
                    lk.addAttribute(attr.getName());
                }
            }
            table.setPrimaryKey(pk);
            table.setLastModifiedKey(lk);
        }

    }

    private void addTenantToTable(Table table, String customer) {
        Tenant tenant = getTenant(customer);
        table.setTenant(tenant);

        List<Attribute> attributes = table.getAttributes();
        for (Attribute attribute : attributes) {
            log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
        }

    }

    private void addExtractToTable(Table table, String path) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(path);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        table.addExtract(e);
    }

    private PrimaryKey createPrimaryKey() {
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        return pk;
    }

    private LastModifiedKey createLastModifiedKey() {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        return lk;
    }

    private Tenant getTenant(String customer) {
        return tenantService.findByTenantId(CustomerSpace.parse(customer).toString());
    }
}
