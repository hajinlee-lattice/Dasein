package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class TableEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TableEntityMgrImplTestNG.class);

    @Autowired
    private MetadataService metadataService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        metadataService.deleteTableAndCleanup(CustomerSpace.parse(customerSpace2), TABLE2);
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
        Table t = tableEntityMgr.findByName(TABLE2);
        assertNull(t);
    }

    @Test(groups = "functional")
    public void testCreateRetry() {
        try {
            Table table = createTable(tenantEntityMgr.findByTenantId(customerSpace2), TABLE2,
                    tableLocation2.append(TABLE2).toString());
            table.setDisplayName(null); // this will fail a NOT NULL constraint
            metadataService.createTable(CustomerSpace.parse(customerSpace1), table);
        } catch (Exception e) {
            Throwable inner = e.getCause();
            assertTrue(inner.getMessage().contains("displayName"));
        }
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findAll(String customerSpace, String tableName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        List<Table> tables = tableEntityMgr.findAll();

        assertEquals(tables.size(), 1);
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findByName(String customerSpace, String tableName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        Table table = tableEntityMgr.findByName(tableName);
        assertNull(table.getRetentionPolicy());
        Date updateTime = table.getUpdated();
        assertNotNull(updateTime);
        addDataRules(table);
        try {
            // sleep 1s let the update time different
            Thread.sleep(1000l);
        } catch (InterruptedException e) {

        }
        metadataService.updateTable(CustomerSpace.parse(customerSpace), table);
        validateTable(table);

        Table retrievedTable = tableEntityMgr.findByName(table.getName());
        assertEquals(retrievedTable.getDataRules().size(), 3);

        String serializedStr = JsonUtils.serialize(retrievedTable);

        Table deserializedTable = JsonUtils.deserialize(serializedStr, Table.class);
        validateTable(deserializedTable);
        assertNotEquals(updateTime, retrievedTable.getUpdated());
    }

    @Test(groups = "functional")
    public void testCountAttributes() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);

        Long attributeCnt = tableEntityMgr.countAttributesByTable_Pid(table.getPid());
        log.info("Attribute Count for table {} - {} ", TABLE1, attributeCnt);
        assertEquals(table.getAttributes().size(), attributeCnt.intValue());
    }

    @Test(groups = "functional")
    public void testFindAttributes() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);

        List<Attribute> attributes = tableEntityMgr.findAttributesByTable_Pid(table.getPid(), null);
        log.info("Attribute List Size for table {} - {} ", TABLE1, attributes.size());
        assertEquals(table.getAttributes().size(), attributes.size());

        attributes = tableEntityMgr.findAttributesByTable_Pid(table.getPid(),
                PageRequest.of(0, CollectionUtils.size(attributes)));
        log.info("Attribute List Size for table {} - {} ", TABLE1, attributes.size());
        assertEquals(table.getAttributes().size(), attributes.size());

        List<Attribute> paginatedAttrs = new ArrayList<>();
        IntStream.range(0, (int) Math.ceil(attributes.size() / 5.0)).forEach(page -> {
            List<Attribute> currPage = tableEntityMgr.findAttributesByTable_Pid(table.getPid(),
                    PageRequest.of(page, 5));
            paginatedAttrs.addAll(currPage);
            log.info("Attribute List by page {} - {} - Results: {} ", page, currPage.size(), currPage);
        });
        assertEquals(paginatedAttrs.size(), attributes.size());
    }

    private void addDataRules(Table table) {
        List<DataRule> dataRules = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            DataRule rule = new DataRule("rule" + i);
            rule.setTable(table);

            List<String> columnsToReview = new ArrayList<>();
            columnsToReview.add("Column" + i);
            rule.setFlaggedColumnNames(columnsToReview);
            rule.setDescription("desc");
            rule.setEnabled(true);
            rule.setMandatoryRemoval(false);
            Map<String, Object> properties = new HashMap<>();
            properties.put("customdomains", "a.com, b.com, c.com");
            rule.setProperties(properties);
            dataRules.add(rule);
        }
        table.setDataRules(dataRules);
    }

    private void validateTable(Table table) {
        List<Attribute> attrs = table.getAttributes();

        assertEquals(attrs.get(3).getApprovedUsage().get(0), "Model");
        assertEquals(attrs.get(3).getDataSource().get(0), "DerivedColumns");
        assertEquals(attrs.get(3).getCategory(), "Firmographics");
        assertEquals(attrs.get(3).getDataType(), "Int");
        assertEquals(attrs.get(3).getStatisticalType(), "ratio");
        assertEquals(attrs.get(3).getFundamentalType(), "numeric");
        assertEquals(attrs.get(3).getTags().get(0), "External");
        assertEquals(attrs.get(3).getSourceLogicalDataType(), "Integer");
        assertEquals(attrs.get(3).getApprovedUsage().get(0), "Model");
        assertEquals(attrs.get(3).getFundamentalType(), "numeric");
        assertEquals(attrs.get(3).getStatisticalType(), "ratio");
        assertEquals(attrs.get(3).getDataSource().get(0), "DerivedColumns");
    }

    private Table updateTableRetentionPolicy(Table table, int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(count, retentionPolicyTimeUnit);
        tableEntityMgr.updateTableRetentionPolicy(table.getName(), retentionPolicy);
        return tableEntityMgr.findByName(table.getName());
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void testTableRetentionPolicy(String customerSpace, String tableName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        Table table = tableEntityMgr.findByName(tableName);
        assertNull(table.getRetentionPolicy());
        Table result = updateTableRetentionPolicy(table, 2, RetentionPolicyTimeUnit.DAY);
        assertEquals(result.getRetentionPolicy(), "KEEP_2_DAYS");
        result = updateTableRetentionPolicy(table, -1, RetentionPolicyTimeUnit.WEEK);
        assertEquals(result.getRetentionPolicy(), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        result = updateTableRetentionPolicy(table, 1, RetentionPolicyTimeUnit.YEAR);
        assertEquals(result.getRetentionPolicy(), "KEEP_1_YEAR");

        List<Table> tables = tableEntityMgr.findAllWithExpiredRetentionPolicy(0, 10);
        assertTrue(tables.size() > 0);

        Map<String, RetentionPolicy> policyMap = new HashMap<>();
        policyMap.put(tableName, RetentionPolicyUtil.toRetentionPolicy(3, RetentionPolicyTimeUnit.WEEK));
        tableEntityMgr.updateTableRetentionPolicies(policyMap);
        result = tableEntityMgr.findByName(tableName);
        assertEquals(result.getRetentionPolicy(), "KEEP_3_WEEKS");
    }

    @Test(groups = "functional", dataProvider = "tableProvider", dependsOnMethods = {"findAll", "findByName", "testTableRetentionPolicy"})
    public void testClone(String customerSpace, String tableName) throws IOException {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        Table table = tableEntityMgr.findByName(tableName);
        String extractPath = "/tmp/data.txt";
        HdfsUtils.writeToFile(yarnConfiguration, extractPath, "test data\ntest data");
        Extract e1 = new Extract();
        e1.setName("extract");
        e1.setPath("/tmp/data.txt");
        e1.setExtractionTimestamp(new Date().getTime());
        e1.setProcessedRecords(2L);
        tableEntityMgr.addExtract(table, e1);

        Table clone = tableEntityMgr.clone(table.getName(), false);
        assertFalse(CollectionUtils.isEmpty(clone.getExtracts()));
        tableEntityMgr.deleteTableAndCleanupByName(clone.getName());

        clone = tableEntityMgr.clone(table.getName(), true);
        assertTrue(CollectionUtils.isEmpty(clone.getExtracts()));
        tableEntityMgr.deleteTableAndCleanupByName(clone.getName());
    }

    @Test(groups = "functional", dataProvider = "tableProvider", dependsOnMethods = "testClone")
    public void deleteTableAndCleanup(String customerSpace, String tableName) throws IOException {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        Table table = tableEntityMgr.findByName(tableName);
        String extractPath = "/tmp/data.txt";
        HdfsUtils.writeToFile(yarnConfiguration, extractPath, "test data\ntest data");
        Extract e1 = new Extract();
        e1.setName("extract");
        e1.setPath("/tmp/data.txt");
        e1.setExtractionTimestamp(new Date().getTime());
        e1.setProcessedRecords(2L);
        tableEntityMgr.addExtract(table, e1);
        tableEntityMgr.deleteTableAndCleanupByName(table.getName());
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, extractPath));
    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { { customerSpace1, TABLE1 }, { customerSpace2, TABLE1 }, };
    }

}
