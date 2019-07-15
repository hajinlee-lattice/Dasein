package com.latticeengines.metadata.functionalframework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.hive.util.HiveUtils;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml",
        "classpath:common-testclient-env-context.xml", "classpath:metadata-aspects-context.xml" })
@Listeners({ GlobalAuthCleanupTestListener.class })
public class MetadataFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(MetadataFunctionalTestNGBase.class);

    protected String customerSpace1;
    protected String customerSpace2;
    protected static final String TABLE1 = "Account1";
    protected static final String TABLE2 = "Account2";
    protected static final String TABLE_RESOURCE1 = "com/latticeengines/metadata/controller/Account1";
    protected static final String TABLE_RESOURCE2 = "com/latticeengines/metadata/controller/Account2";

    protected Path tableLocation1;
    protected Path tableLocation2;

    @Autowired
    protected Configuration yarnConfiguration;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Value("${common.test.microservice.url}")
    private String hostPort;

    @Value("${metadata.hive.enabled:false}")
    private boolean hiveEnabled;

    @Autowired
    protected AttributeEntityMgr attributeEntityMgr;

    @Autowired
    protected TableEntityMgr tableEntityMgr;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;

    @Autowired
    private DataSource hiveDataSource;

    @Autowired
    protected TableTypeHolder tableTypeHolder;

    @Autowired
    protected GlobalAuthFunctionalTestBed functionalTestBed;

    @Autowired
    private MetadataService metadataService;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void setup() {
        functionalTestBed.bootstrap(2);
        customerSpace1 = functionalTestBed.getTestTenants().get(0).getId();
        customerSpace2 = functionalTestBed.getTestTenants().get(1).getId();

        tableLocation1 = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace1), "x.y.z");
        tableLocation2 = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace2), "x.y.z");

        if (hiveEnabled) {
            dropAllHiveTables();
        }
        copyExtractsToHdfs();
        setupTables();
    }

    protected void cleanup() {
        tenantEntityMgr.delete(tenantEntityMgr.findByTenantId(customerSpace1));
        tenantEntityMgr.delete(tenantEntityMgr.findByTenantId(customerSpace2));
    }

    private void copyExtractsToHdfs() {
        log.info("copyExtractsToHdfs");
        try {
            Assert.assertNotEquals(yarnConfiguration.get("fs.defaultFS"), "file:///",
                    "$HADOOP_HOME/etc/hadoop must be on the classpath, and configured to use a hadoop cluster in order for this test to run");
            HdfsUtils.rmdir(yarnConfiguration, tableLocation1.toString());
            HdfsUtils.mkdir(yarnConfiguration, tableLocation1.toString());
            HdfsUtils.rmdir(yarnConfiguration, tableLocation2.toString());
            HdfsUtils.mkdir(yarnConfiguration, tableLocation2.toString());
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, TABLE_RESOURCE1, tableLocation1.toString());
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, TABLE_RESOURCE2, tableLocation2.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup hdfs for metadata test", e);
        }
    }

    private void setupTables() {
        log.info("setupTables");
        Tenant tenant1 = tenantEntityMgr.findByTenantId(customerSpace1);
        Tenant tenant2 = tenantEntityMgr.findByTenantId(customerSpace2);

        // Tenant1, Type=DATATABLE
        Table tbl = createTable(tenant1, TABLE1, tableLocation1.append(TABLE1).toString());
        createTable(tenant1, tbl, false);
        // Tenant1, Type=IMPORTTABLE
        createTable(tenant1, tbl, true);

        tbl = createTable(tenant2, TABLE1, tableLocation1.append(TABLE1).toString());
        // Tenant2, Type=DATATABLE
        createTable(tenant2, tbl, false);
        // Tenant2, Type=IMPORTTABLE
        createTable(tenant2, tbl, true);
    }

    private void dropAllHiveTables() {
        log.info("dropAllHiveTables");
        try (Connection connection = hiveDataSource.getConnection()) {

            List<String> tables = getAllHiveTables(connection);
            for (String table : tables) {
                dropTable(connection, table);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getAllHiveTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SHOW TABLES");
            ResultSet results = stmt.getResultSet();
            while (results.next()) {
                tables.add(results.getString("tab_name"));
            }
        }
        return tables;
    }

    private void dropTable(Connection connection, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(HiveUtils.getDropStatement(tableName, true));
        }
    }

    private void createTable(Tenant tenant, Table table, boolean isImport) {
        if (isImport) {
            table.setTableType(TableType.IMPORTTABLE);
        } else {
            table.setTableType(TableType.DATATABLE);
        }
        metadataService.createTable(CustomerSpace.parse(tenant.getId()), table);
    }

    protected Table createTable(Tenant tenant, String tableName, String path) {
        path = path + "/*.avro";
        Table table = MetadataConverter.getTable(yarnConfiguration, path, "Id", "CreatedDate");
        table.setName(tableName);
        table.setTenant(tenant);
        table.setNamespace("x.y.z");
        Attribute attribute = table.getAttributes().get(3);
        attribute.setSourceLogicalDataType("Integer");
        attribute.setApprovedUsage(ModelingMetadata.MODEL_APPROVED_USAGE);
        attribute.setCategory("Firmographics");
        attribute.setDataType("Int");
        attribute.setFundamentalType("numeric");
        attribute.setStatisticalType("ratio");
        attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
        attribute.setDataSource("[DerivedColumns]");
        attribute.addValidator(new RequiredIfOtherFieldIsEmpty("Test"));
        return table;
    }
}
