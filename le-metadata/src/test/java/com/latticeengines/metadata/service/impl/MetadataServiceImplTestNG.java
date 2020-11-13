package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyUpdateDetail;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;

public class MetadataServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MetadataServiceImplTestNG.class);

    @Inject
    private MetadataService mdService;

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private PrestoDbService prestoDbService;

    @Inject
    private AthenaService athenaService;

    @Inject
    private DataUnitService dataUnitService;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void getTable(String customerSpace, String tableName) {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
        assertNotNull(table);
        assertEquals(table.getRetentionPolicy(), null);
        assertEquals(table.getName(), tableName);
        assertNotNull(table.getLastModifiedKey());
        assertNotNull(table.getPrimaryKey());
        assertNotNull(table.getAttributes());
        log.info("Attribute Count for Table: {} - {}", tableName, table.getAttributes().size());
        assertTrue(table.getAttributes().size() > 0);
        assertEquals(table.getStorageMechanism().getName(), "HDFS");
        assertTrue(table.getAttributes().size() == table.getColumnMetadata().size());

        // Get without attributes
        table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, false);
        assertNotNull(table);
        assertNotNull(table.getAttributes());
        assertTrue(table.getAttributes().size() == 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTable" })
    public void getTableAttributes() {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1, true);
        assertNotNull(table.getAttributes());

        Pageable pageReq = PageRequest.of(0, 50000);
        List<Attribute> colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1,
                pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), table.getAttributes().size());

        // With Pageable as Null
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, null);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), table.getAttributes().size());

        pageReq = PageRequest.of(0, 10);
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), 10);

        pageReq = PageRequest.of(0, 10, Direction.ASC, "displayName");
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), 10);
    }

    @Test(groups = "functional")
    public void getTables() {
        List<Table> tables = mdService.getTables(CustomerSpace.parse(customerSpace1));
        assertEquals(tables.size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTable" })
    public void registerPrestoDataUnit() {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpace1);
        List<Table> tables = mdService.getTables(customerSpace);
        assertEquals(tables.size(), 1);

        // test first registration
        PrestoDataUnit prestoDataUnit = mdService.registerPrestoDataUnit(customerSpace, TABLE1);
        Assert.assertNotNull(prestoDataUnit);
        Assert.assertEquals(prestoDataUnit.getTenant(), customerSpace.getTenantId());
        Assert.assertEquals(prestoDataUnit.getName(), TABLE1);
        String clusterId = prestoConnectionService.getClusterId();
        String tableName = prestoDataUnit.getPrestoTableName(clusterId);
        Assert.assertTrue(prestoDbService.tableExists(tableName));
        DataSource dataSource = prestoConnectionService.getPrestoDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM " + tableName, Long.class);
        Assert.assertEquals(count, Long.valueOf(550));

        // test idempotent
        prestoDataUnit = mdService.registerPrestoDataUnit(customerSpace, TABLE1);
        Assert.assertNotNull(prestoDataUnit);
        Assert.assertEquals(prestoDataUnit.getTenant(), customerSpace.getTenantId());
        Assert.assertEquals(prestoDataUnit.getName(), TABLE1);
        String tableName2 = prestoDataUnit.getPrestoTableName(clusterId);
        Assert.assertEquals(tableName2, tableName);
        Assert.assertTrue(prestoDbService.tableExists(tableName));
        Long count2 = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM " + tableName, Long.class);
        Assert.assertEquals(count2, count);

        prestoDbService.deleteTableIfExists(tableName);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTable" })
    public void registerAthenaDataUnit() {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpace1);
        List<Table> tables = mdService.getTables(customerSpace);
        assertEquals(tables.size(), 1);

        // setup s3 data unit
        String s3Prefix = leStack + "/MetadataServiceImplTest/" + TABLE1;
        String s3Protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        String s3Dir = s3Protocol + "://" + s3Bucket + "/" + s3Prefix;
        Table table = mdService.getTable(customerSpace, TABLE1);
        String hdfsDir = table.toHdfsDataUnit("table").getPath();
        hdfsDir = PathUtils.toParquetOrAvroDir(hdfsDir);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
                HdfsUtils.rmdir(yarnConfiguration, s3Dir);
            }
            log.info("Copying from {} to {}", hdfsDir, s3Dir);
            HdfsUtils.copyFiles(yarnConfiguration, hdfsDir, s3Dir);
        } catch (Exception e) {
            Assert.fail("Failed to move test data to S3", e);
        }

        S3DataUnit s3DataUnit = new S3DataUnit();
        s3DataUnit.setTenant(customerSpace.getTenantId());
        s3DataUnit.setName(TABLE1);
        s3DataUnit.setBucket(s3Bucket);
        s3DataUnit.setPrefix(s3Prefix);
        s3DataUnit.setDataFormat(DataUnit.DataFormat.AVRO);
        String tableName = null;
        AthenaDataUnit athenaDataUnit = null;
        try {
            dataUnitService.createOrUpdateByNameAndStorageType(s3DataUnit);
            RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                    Collections.singleton(AssertionError.class), null);
            SleepUtils.sleep(500L);
            retry.execute(ctx -> {
                Assert.assertNotNull(dataUnitService.findByNameTypeFromReader(TABLE1, DataUnit.StorageType.S3));
                return true;
            });
            athenaDataUnit = mdService.registerAthenaDataUnit(customerSpace, TABLE1);
            Assert.assertNotNull(athenaDataUnit);
            Assert.assertEquals(athenaDataUnit.getTenant(), customerSpace.getTenantId());
            Assert.assertEquals(athenaDataUnit.getName(), TABLE1);
            tableName = athenaDataUnit.getAthenaTable();
            Assert.assertTrue(athenaService.tableExists(tableName));
            Long count = athenaService.queryObject("SELECT COUNT(1) FROM " + tableName, Long.class);
            Assert.assertEquals(count, Long.valueOf(550));

            dataUnitService.delete(athenaDataUnit);
            Assert.assertFalse(athenaService.tableExists(tableName));
        } finally {
            dataUnitService.delete(s3DataUnit);
            if (athenaDataUnit != null) {
                dataUnitService.delete(athenaDataUnit);
            }
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
                    HdfsUtils.rmdir(yarnConfiguration, s3Dir);
                }
            } catch (Exception e) {
                Assert.fail("Failed to move test data to S3", e);
            }
            if (StringUtils.isNotBlank(tableName)) {
                athenaService.deleteTableIfExists(tableName);
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "getTables" })
    public void addStorageMechanism() {
        Table table = mdService.getTables(CustomerSpace.parse(customerSpace1)).get(0);
        JdbcStorage jdbcStorage = new JdbcStorage();
        jdbcStorage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        jdbcStorage.setTableNameInStorage("TABLE1_IN_REDSHIFT");
        mdService.setStorageMechanism(CustomerSpace.parse(customerSpace1), table.getName(), jdbcStorage);

        Table retrievedTable = mdService.getTables(CustomerSpace.parse(customerSpace1)).get(0);
        JdbcStorage storageMechanism = (JdbcStorage) retrievedTable.getStorageMechanism();
        assertEquals(storageMechanism.getDatabaseName(), JdbcStorage.DatabaseName.REDSHIFT);
    }

    private Table updateTableRetentionPolicy(CustomerSpace customerSpace, Table table, int count, RetentionPolicyTimeUnit retentionPolicyTimeUnit) {
        RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(count, retentionPolicyTimeUnit);
        mdService.updateTableRetentionPolicy(customerSpace, table.getName(), retentionPolicy);
        return mdService.getTable(customerSpace, table.getName());
    }

    @Test(groups = "functional", dataProvider = "tableProvider", dependsOnMethods = "addStorageMechanism")
    public void testTableRetentionPolicy(String customerSpaceStr, String tableName) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        Table table = mdService.getTable(customerSpace, tableName);
        assertNull(table.getRetentionPolicy());
        Table result = updateTableRetentionPolicy(customerSpace, table, 2, RetentionPolicyTimeUnit.DAY);
        assertEquals(result.getRetentionPolicy(), "KEEP_2_DAYS");
        result = updateTableRetentionPolicy(customerSpace, table, -1, RetentionPolicyTimeUnit.WEEK);
        assertEquals(result.getRetentionPolicy(), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        result = updateTableRetentionPolicy(customerSpace, table, 1, RetentionPolicyTimeUnit.YEAR);
        assertEquals(result.getRetentionPolicy(), "KEEP_1_YEAR");

        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        result = updateTableRetentionPolicy(customerSpace, table, 2, RetentionPolicyTimeUnit.MONTH);
        assertEquals(result.getTableType(), TableType.IMPORTTABLE);
        assertEquals(result.getRetentionPolicy(), "KEEP_2_MONTHS");
        tableTypeHolder.setTableType(TableType.DATATABLE);
        List<Table> tables = mdService.findAllWithExpiredRetentionPolicy(0, 10);
        assertTrue(tables.size() > 0);

        RetentionPolicyUpdateDetail retentionPolicyUpdateDetail = new RetentionPolicyUpdateDetail();
        retentionPolicyUpdateDetail.setTableNames(Lists.newArrayList(tableName));
        retentionPolicyUpdateDetail.setRetentionPolicy(RetentionPolicyUtil.toRetentionPolicy(3, RetentionPolicyTimeUnit.WEEK));
        mdService.updateTableRetentionPolicies(customerSpace, retentionPolicyUpdateDetail);
        result = mdService.getTable(customerSpace, tableName);
        assertEquals(result.getRetentionPolicy(), "KEEP_3_WEEKS");
    }

    @Test(groups = "functional", dependsOnMethods = { "testTableRetentionPolicy" })
    public void cloneTable() throws IOException {
        Table cloned = mdService.cloneTable(CustomerSpace.parse(customerSpace1), TABLE1, false);
        assertNotNull(cloned);
        List<Extract> extracts = cloned.getExtracts();
        Assert.assertNotNull(extracts);
        Assert.assertEquals(extracts.size(), 1);
        Extract extract = extracts.get(0);
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, extract.getPath());
        Assert.assertNotNull(files);
        Assert.assertEquals(files.size(), 2);
        String[] expected = { "Extract1_0.avro", "Extract2_1.avro" };
        Set<String> expectedSet = new HashSet<>(Arrays.asList(expected));
        files.forEach(file -> {
            String fileName = new org.apache.hadoop.fs.Path(file).getName();
            Assert.assertTrue(expectedSet.contains(fileName));
        });
    }

    @Test(groups = "functional", dependsOnMethods = { "cloneTable" })
    public void renameTable() {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1, true);
        assertNotNull(table);
        String newName = table.getName() + "-rename1";
        mdService.renameTable(CustomerSpace.parse(customerSpace1), table.getName(), newName);
        Table newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), newName, true);
        assertEquals(newTable.getPid(), table.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "renameTable" })
    public void fixTableAttribute() {
        Table newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1 + "-rename1", true);
        Long tablePid = newTable.getPid();
        Assert.assertNotNull(newTable);
        Attribute attr1 = newTable.getAttribute("OwnerId");
        Attribute attr2 = newTable.getAttribute("CreatedDate");

        Assert.assertNotNull(attr1);
        Assert.assertNotNull(attr2);
        Assert.assertEquals(attr1.getPhysicalDataType(), "string");
        Assert.assertTrue(StringUtils.isEmpty(attr1.getFundamentalType()));
        Assert.assertNull(attr2.getDateFormatString());
        AttributeFixer attributeFixer1 = new AttributeFixer();
        attributeFixer1.setName("OwnerId");
        attributeFixer1.setPhysicalDataType("Int");
        attributeFixer1.setFundamentalType(FundamentalType.NUMERIC);

        AttributeFixer attributeFixer2 = new AttributeFixer();
        attributeFixer2.setName("CreatedDate");
        attributeFixer2.setDateFormat("MM/DD/YYYY");

        mdService.fixAttributes(CustomerSpace.parse(customerSpace1), newTable.getName(),
                Arrays.asList(attributeFixer1, attributeFixer2));
        newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1 + "-rename1", true);
        Assert.assertEquals(newTable.getPid(), tablePid);

        attr1 = newTable.getAttribute("OwnerId");
        attr2 = newTable.getAttribute("CreatedDate");

        Assert.assertNotNull(attr1);
        Assert.assertNotNull(attr2);
        Assert.assertEquals(attr1.getPhysicalDataType(), "Int");
        Assert.assertEquals(attr1.getFundamentalType(), FundamentalType.NUMERIC.name());
        Assert.assertEquals(attr2.getDateFormatString(), "MM/DD/YYYY");

    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { { customerSpace1, TABLE1 }, { customerSpace2, TABLE1 }, };
    }
}
