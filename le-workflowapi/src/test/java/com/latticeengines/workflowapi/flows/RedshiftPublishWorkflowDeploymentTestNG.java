package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class RedshiftPublishWorkflowDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace
            .parse(RedshiftPublishWorkflowDeploymentTestNG.class.getSimpleName());

    protected static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/redshiftpublish/avrofiles";

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.test.microservice.url}")
    protected String microserviceHostPort;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws Exception {
        cleanUpAfterWorkflow();
    }

    protected void setupForWorkflow() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        MultiTenantContext.setTenant(tenant);
        assertNotNull(MultiTenantContext.getTenant());
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
    }

    protected void cleanUpAfterWorkflow() throws Exception {
        deleteTenantByRestCall(DEMO_CUSTOMERSPACE.toString());
        cleanCamille(DEMO_CUSTOMERSPACE);
        cleanHdfs(DEMO_CUSTOMERSPACE);
    }

    public HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey("LatticeAccountId");
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Collections.<String> singletonList("LatticeAccountId"));
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }

    @Test(groups = "deployment")
    public void initialLoad() throws Exception {
        String localFilePath = getClass().getClassLoader().getResource(RESOURCE_BASE + "/a.avro").getPath();
        String tableName = AvroUtils.readSchemaFromLocalFile(localFilePath).getName();
        String dest = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), DEMO_CUSTOMERSPACE)
                .append(tableName).append("a.avro").toString();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFilePath, dest);
        Table table = MetadataConverter.getTable(yarnConfiguration, dest);
        metadataProxy.createTable(DEMO_CUSTOMERSPACE.toString(), table.getName(), table);
        Map<BusinessEntity, Table> sourceTables = new HashMap<>();
        sourceTables.put(BusinessEntity.Account, table);
        HdfsToRedshiftConfiguration exportConfig = createExportBaseConfig();
        exportConfig.setCreateNew(true);
        exportConfig.setAppend(true);
        RedshiftPublishWorkflowConfiguration.Builder builder = new RedshiftPublishWorkflowConfiguration.Builder();
        builder.hdfsToRedshiftConfiguration(exportConfig);
        builder.sourceTables(sourceTables);
        builder.customer(DEMO_CUSTOMERSPACE);
        builder.microServiceHostPort(microserviceHostPort);
        RedshiftPublishWorkflowConfiguration config = builder.build();

        WorkflowExecutionId workflowId = workflowService.start(config);
        waitForCompletion(workflowId);
        verify(table.getName(), 5);
        HdfsUtils.rmdir(yarnConfiguration, dest);
        metadataProxy.deleteTable(DEMO_CUSTOMERSPACE.toString(),
                String.join("_", DEMO_CUSTOMERSPACE.getTenantId(), BusinessEntity.Account.name()));
    }

    @Test(groups = "deployment", dependsOnMethods = "initialLoad")
    public void updateRows() throws Exception {
        String localFilePath = getClass().getClassLoader().getResource(RESOURCE_BASE + "/b.avro").getPath();
        String tableName = AvroUtils.readSchemaFromLocalFile(localFilePath).getName();
        String dest = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), DEMO_CUSTOMERSPACE)
                .append(tableName).append("b.avro").toString();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFilePath, dest);
        Table table = MetadataConverter.getTable(yarnConfiguration, dest);
        metadataProxy.createTable(DEMO_CUSTOMERSPACE.toString(), table.getName(), table);
        Map<BusinessEntity, Table> sourceTables = new HashMap<>();
        sourceTables.put(BusinessEntity.Account, table);

        HdfsToRedshiftConfiguration exportConfig = createExportBaseConfig();
        RedshiftPublishWorkflowConfiguration.Builder builder = new RedshiftPublishWorkflowConfiguration.Builder();
        builder.hdfsToRedshiftConfiguration(exportConfig);
        builder.sourceTables(sourceTables);
        builder.customer(DEMO_CUSTOMERSPACE);
        builder.microServiceHostPort(microserviceHostPort);
        RedshiftPublishWorkflowConfiguration config = builder.build();
        WorkflowExecutionId workflowId = workflowService.start(config);
        waitForCompletion(workflowId);
        verify(table.getName(), 8);
        HdfsUtils.rmdir(yarnConfiguration, dest);
    }

    private void verify(String table, int size) {
        String sql = String.format("SELECT * FROM %s LIMIT 10", table);
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() == size, "Got 0 result by querying [" + sql + "]");
    }

}
