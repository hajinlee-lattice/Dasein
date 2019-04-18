package com.latticeengines.metadata.functionalframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-metadata-context.xml", "classpath:common-testclient-env-context.xml", "classpath:metadata-aspects-context.xml" })
@Listeners({ GlobalAuthCleanupTestListener.class })
public class MetadataDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(MetadataDeploymentTestNGBase.class);

    protected Tenant tenant1;
    protected Tenant tenant2;
    protected String customerSpace1;
    protected String customerSpace2;

    protected static final String TABLE1 = "Account1";
    protected static final String TABLE2 = "Account2";
    protected static final String TABLE_RESOURCE1 = "com/latticeengines/metadata/controller/Account1";
    protected static final String TABLE_RESOURCE2 = "com/latticeengines/metadata/controller/Account2";

    protected Path tableLocation1;
    protected Path tableLocation2;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    protected MetadataProxy metadataProxy;

    protected void setup() {
        deploymentTestBed.bootstrap(2);
        tenant1 = deploymentTestBed.getMainTestTenant();
        tenant2 = deploymentTestBed.getTestTenants().get(1);

        customerSpace1 = tenant1.getId();
        customerSpace2 = tenant2.getId();

        tableLocation1 = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace1), "x.y.z");
        tableLocation2 = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace2), "x.y.z");

        copyExtractsToHdfs();
        setupTables();
    }

    protected void cleanup() {
        deploymentTestBed.deleteTenant(tenant1);
        deploymentTestBed.deleteTenant(tenant2);
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

    private void createTable(Tenant tenant, Table table, boolean isImport) {
        if (isImport) {
            table.setTableType(TableType.IMPORTTABLE);
            metadataProxy.createImportTable(tenant.getId(), table.getName(), table);
        } else {
            table.setTableType(TableType.DATATABLE);
            metadataProxy.createTable(tenant.getId(), table.getName(), table);
        }
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
