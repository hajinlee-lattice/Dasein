package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@Component
public class RegisterLocalTestBucketedAccountTableTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RegisterLocalTestBucketedAccountTableTestNG.class);

    private static final String LOCALTEST_TENANT = "LocalTest";
    private static final String S3_DIR = "le-dev/LocalTest";
    private static final String S3_VERSION = "4";

    @Inject
    private MetadataService mdService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    private DataUnitEntityMgr dataUnitEntityMgr;

    private String customerSpace;
    private Tenant localTenant;

    @SuppressWarnings("groupsTestNG")
    @Test(groups = "registertable")
    public void bootstrapMetadata() throws Exception {
        createLocalTestTenant();
        bootstrapDataCollection();
        registerTables();
        registerDynamoDataUnit();
    }

    private void createLocalTestTenant() {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(LOCALTEST_TENANT).toString());
        if (tenant == null) {
            Assert.fail(LOCALTEST_TENANT + " tenant does not exists.");
        }
        localTenant = tenant;
        customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        MultiTenantContext.setTenant(tenant);
    }

    private void bootstrapDataCollection() throws IOException {
        DataCollection collection = dataCollectionService.getDataCollection(customerSpace, "");
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, "stats_container.json");
        ObjectMapper om = new ObjectMapper();
        StatisticsContainer container = om.readValue(is, StatisticsContainer.class);
        container.setName(NamingUtils.timestamp("Stats"));
        container.setVersion(collection.getVersion());
        dataCollectionService.addStats(customerSpace, collection.getName(), container);
    }

    private void registerTables() {
        List<Runnable> runnables = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.values()) {
            Runnable runnable = registerServingStore(entity);
            if (runnable != null) {
                runnables.add(runnable);
            }
        }
        ExecutorService threadPool = ThreadPoolUtils.getFixedSizeThreadPool("register-serving-store", 4);
        ThreadPoolUtils.runRunnablesInParallel(threadPool, runnables, 60, 3);
    }

    private Runnable registerServingStore(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, role.name() + ".json");
        if (is != null) {
            return () -> {
                Table table;
                try {
                    ObjectMapper om = new ObjectMapper();
                    List list = om.readValue(is, List.class);
                    table = JsonUtils.convertList(list, Table.class).get(0);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse table from " + role.name() + ".json.", e);
                }
                String tableName = servingStoreName(entity);
                table.setName(tableName);
                table.setTableType(TableType.DATATABLE);
                if (mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName) == null) {
                    mdService.createTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
                } else {
                    mdService.updateTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
                }
                Table tableFromDB = mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName);
                Assert.assertEquals(table.getAttributes().size(), tableFromDB.getAttributes().size());
                DataCollection.Version version = dataCollectionService.getDataCollection(customerSpace, "")
                        .getVersion();
                dataCollectionService.upsertTable(customerSpace, "", tableName, role, version);
            };
        } else {
            return null;
        }
    }

    private void registerDynamoDataUnit() throws IOException {
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION,
                "ConsolidatedAccount_Dynamo.json");
        if (is != null) {
            ObjectMapper om = new ObjectMapper();
            DynamoDataUnit unit = om.readValue(is, DynamoDataUnit.class);
            if (StringUtils.isBlank(unit.getLinkedTable())) {
                unit.setLinkedTable(unit.getName());
            }
            if (StringUtils.isBlank(unit.getLinkedTenant())) {
                unit.setLinkedTenant(unit.getTenant());
            }
            unit.setName(servingStoreName(BusinessEntity.Account));
            unit.setTenant("LocalTest");
            dataUnitEntityMgr.createOrUpdateByNameAndStorageType("LocalTest", unit);
            log.info("Registered data unit: " + JsonUtils.serialize(unit));
        }
    }

    private String servingStoreName(BusinessEntity entity) {
        return String.format("LocalTest_%s_%s", entity.name(), S3_VERSION).toLowerCase();
    }

    @Test(groups = "registertable", dependsOnMethods = {
            "bootstrapMetadata" }, invocationCount = 2, threadPoolSize = 8, enabled = false)
    public void registerTablesLoadTest() throws IOException {
        String threadName = Thread.currentThread().getName();
        log.info("Load Testing from Thread: {}", threadName);
        MultiTenantContext.setTenant(localTenant);

        for (TableRoleInCollection role : Arrays.asList(TableRoleInCollection.BucketedAccount)) {
            InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, role.name() + ".json");
            ObjectMapper om = new ObjectMapper();
            Table table = om.readValue(is, Table.class);
            String tableName = table.getName() + "-" + threadName;
            table.setName(tableName);
            log.info("Created Table metadata from test file. TableName: {}", tableName);
            table.setTableType(TableType.DATATABLE);

            // Save attributes as separate transactions
            List<Attribute> attributes = table.getAttributes();
            table.setAttributes(null);

            if (mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName) == null) {
                mdService.createTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
            } else {
                mdService.updateTable(CustomerSpace.parse(LOCALTEST_TENANT), table);
            }

            Table tableFromDB = mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName);
            Assert.assertEquals(0, tableFromDB.getAttributes().size());

            // Update Attributes
            if (attributes == null) {
                continue;
            }
            int chunkSize = 4000;
            for (int i = 0; (i * chunkSize) < attributes.size(); i++) {
                List<Attribute> subList = attributes.subList(i * chunkSize,
                        Math.min(i * chunkSize + chunkSize, attributes.size()));
                if (log.isInfoEnabled()) {
                    log.info("Created sublist {} with Size {}", i, subList.size());
                }
                mdService.addAttributes(CustomerSpace.parse(LOCALTEST_TENANT), table.getName(), subList);
            }
            tableFromDB = mdService.getTable(CustomerSpace.parse(LOCALTEST_TENANT), tableName);
            Assert.assertEquals(attributes.size(), tableFromDB.getAttributes().size());

            log.info("Completed Table creation via metadata service. TableName: {}", tableName);
        }
    }

}
