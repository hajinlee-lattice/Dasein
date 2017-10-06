package com.latticeengines.metadata.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import javax.inject.Inject;

import org.junit.Assert;
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
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@Component
public class RegisterLocalTestBucketedAccountTableTestNG extends MetadataFunctionalTestNGBase {

    private static final String LOCALTEST_TENANT = "LocalTest";
    private static final String S3_DIR = "le-dev/LocalTest";
    private static final String S3_VERSION = "1";

    @Inject
    private MetadataService mdService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private TestArtifactService testArtifactService;

    private String customerSpace;

    @SuppressWarnings("groupsTestNG")
    @Test(groups = "registertable")
    public void registerMetadataTable() throws Exception {
        createLocalTestTenant();
        bootstrapDataCollection();
        registerTables();
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
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, "stats_container.json");
        ObjectMapper om = new ObjectMapper();
        StatisticsContainer container = om.readValue(is, StatisticsContainer.class);
        container.setVersion(collection.getVersion());
        dataCollectionService.addStats(customerSpace, collection.getName(), container);
    }

    private void registerTables() throws IOException {
        for (TableRoleInCollection role: Arrays.asList(
                TableRoleInCollection.BucketedAccount, //
                TableRoleInCollection.SortedContact, //
                TableRoleInCollection.SortedProduct, //
                TableRoleInCollection.AggregatedTransaction, //
                TableRoleInCollection.CalculatedPurchaseHistory
        )) {
            InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, role.name() + ".json");
            ObjectMapper om = new ObjectMapper();
            Table table = om.readValue(is, Table.class);
            String tableName = table.getName();
            table.setTableType(TableType.DATATABLE);
            if (mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName) == null) {
                mdService.createTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
            } else {
                mdService.updateTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
            }
            DataCollection.Version version = dataCollectionService.getDataCollection(customerSpace, "").getVersion();
            dataCollectionService.upsertTable(customerSpace, "", tableName, role, version);
        }
    }

}
