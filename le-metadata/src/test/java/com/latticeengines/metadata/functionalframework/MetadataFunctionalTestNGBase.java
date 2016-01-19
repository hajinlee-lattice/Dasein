package com.latticeengines.metadata.functionalframework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.hive.util.HiveUtils;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.GetHttpStatusErrorHandler;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml", "classpath:metadata-aspects-context.xml" })
public class MetadataFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
    private static final Logger log = Logger.getLogger(MetadataFunctionalTestNGBase.class);

    protected static final String CUSTOMERSPACE1 = "T1.T1.T1";
    protected static final String CUSTOMERSPACE2 = "T2.T2.T2";
    protected static final String TABLE1 = "Account1";
    protected static final String TABLE2 = "Account2";
    protected static final String TABLE_RESOURCE1 = "com/latticeengines/metadata/controller/Account1";
    protected static final String TABLE_RESOURCE2 = "com/latticeengines/metadata/controller/Account2";
    protected static final String TABLE_LOCATION1 = "/tmp/metadataFunctionalTestDir/Account1";
    protected static final String TABLE_LOCATION2 = "/tmp/metadataFunctionalTestDir/Account2";

    protected Configuration yarnConfiguration = new Configuration();

    protected RestTemplate restTemplate = new RestTemplate();

    @Value("${metadata.test.functional.api:http://localhost:8080/}")
    private String hostPort;

    @Value("${metadata.hive.enabled:false}")
    private boolean hiveEnabled;

    @Autowired
    protected TableEntityMgr tableEntityMgr;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;

    @Autowired
    private DataSource hiveDataSource;

    @Autowired
    protected TableTypeHolder tableTypeHolder;

    protected SecurityFunctionalTestNGBase securityTestBase = new SecurityFunctionalTestNGBase();

    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = securityTestBase.getAuthHeaderInterceptor();
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = securityTestBase
            .getMagicAuthHeaderInterceptor();
    protected GetHttpStatusErrorHandler statusErrorHandler = securityTestBase.getStatusErrorHandler();

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    public void setup() {
        if (hiveEnabled) {
            dropAllHiveTables();
        }
        copyExtractsToHdfs();
        setupTenantsAndTables();
    }

    private void copyExtractsToHdfs() {
        log.info("copyExtractsToHdfs");
        try {
            Assert.assertNotEquals(
                    yarnConfiguration.get("fs.defaultFS"),
                    "file:///",
                    "$HADOOP_HOME/etc/hadoop must be on the classpath, and configured to use a hadoop cluster in order for this test to run");

            HdfsUtils.rmdir(yarnConfiguration, "/tmp/metadataFunctionalTestDir");
            HdfsUtils.mkdir(yarnConfiguration, "/tmp/metadataFunctionalTestDir");
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, TABLE_RESOURCE1, "/tmp/metadataFunctionalTestDir");
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, TABLE_RESOURCE2, "/tmp/metadataFunctionalTestDir");
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup hdfs for metadata test", e);
        }
    }

    private void setupTenantsAndTables() {
        log.info("setupTenantsAndTables");

        Tenant t1 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE1);
        if (t1 != null) {
            tenantEntityMgr.delete(t1);
        }
        Tenant t2 = tenantEntityMgr.findByTenantId(CUSTOMERSPACE2);
        if (t2 != null) {
            tenantEntityMgr.delete(t2);
        }

        Tenant tenant1 = createTenant(CUSTOMERSPACE1);
        tenantEntityMgr.create(tenant1);

        Tenant tenant2 = createTenant(CUSTOMERSPACE2);
        tenantEntityMgr.create(tenant2);

        // Tenant1, Type=DATATABLE
        Table tbl = createTable(tenant1, TABLE1, TABLE_LOCATION1);
        createTableByRestCall(tenant1, tbl, false);
        // Tenant1, Type=IMPORTTABLE
        createTableByRestCall(tenant1, tbl, true);

        // Tenant2, Type=DATATABLE
        tbl = createTable(tenant2, TABLE1, TABLE_LOCATION1);
        createTableByRestCall(tenant2, tbl, false);
        // Tenant2, Type=IMPORTTABLE
        createTableByRestCall(tenant2, tbl, true);
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

    private void createTableByRestCall(Tenant tenant, Table table, boolean isImport) {
        String urlType = "tables";
        if (isImport) {
            urlType = "importtables";
        }

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", getRestAPIHostPort(), table.getTenant()
                .getId(), urlType, table.getName());
        restTemplate.postForLocation(url, table);
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        return tenant;
    }

    protected Table createTable(Tenant tenant, String tableName, String path) {
        path = path + "/*.avro";
        Table table = MetadataConverter.getTable(yarnConfiguration, path, "Id", "CreatedDate");
        table.setName(tableName);
        table.setTenant(tenant);
        Attribute attribute = table.getAttributes().get(3);
        attribute.setLogicalDataType("Integer");
        attribute.setApprovedUsage(ModelingMetadata.MODEL_APPROVED_USAGE);
        attribute.setCategory("Firmographics");
        attribute.setDataType("Int");
        attribute.setFundamentalType("numeric");
        attribute.setStatisticalType("ratio");
        attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
        attribute.setDataSource("DerivedColumns");

        return table;
    }
}
