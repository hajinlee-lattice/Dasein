package com.latticeengines.metadata.service.impl;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class RegisterLocalTestBucketedAccountTableTestNG extends MetadataFunctionalTestNGBase {

    private static final String LOCALTEST_TENANT = "LocalTest";

    @Autowired
    private MetadataService mdService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private DataCollectionService dataCollectionService;

    private String customerSpace;

    @Test(groups = "registertable")
    public void registerMetadataTable() throws Exception {
        createLocalTestTenant();
        bootstrapDataCollection();
        registerBucketedAccount();
    }

    private void createLocalTestTenant() {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(LOCALTEST_TENANT).toString());
        if (tenant == null) {
            Assert.fail(LOCALTEST_TENANT + " tenant does not exists.");
        }
        customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        MultiTenantContext.setTenant(tenant);
    }

    private void bootstrapDataCollection() throws IOException {
        DataCollection collection = dataCollectionService.getDataCollection(customerSpace, "");
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("dev/stats.json");
        ObjectMapper om = new ObjectMapper();
        StatisticsContainer container = om.readValue(is, StatisticsContainer.class);
        dataCollectionService.addStats(customerSpace, collection.getName(), container);
    }

    private void registerBucketedAccount() throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("dev/BucketedAccount.json");
        ObjectMapper om = new ObjectMapper();
        Table bucketedTable = om.readValue(is, Table.class);
        String tableName = bucketedTable.getName();
        bucketedTable.setTableType(TableType.DATATABLE);
        if (mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName) == null) {
            mdService.createTable(CustomerSpace.parse(LOCALTEST_TENANT), bucketedTable);
        } else {
            mdService.updateTable(CustomerSpace.parse(LOCALTEST_TENANT), bucketedTable);
        }
        dataCollectionService.upsertTable(customerSpace, "", tableName, TableRoleInCollection.BucketedAccount);
    }

}
