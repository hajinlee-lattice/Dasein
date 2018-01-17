package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class RedshiftPublishWorkflowDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/redshiftpublish/avrofiles";

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.test.microservice.url}")
    protected String microserviceHostPort;

    private String targetTableName;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        targetTableName = String.join("_", mainTestCustomerSpace.getTenantId(), BusinessEntity.Account.name());
    }

    @Test(groups = "workflow")
    public void initialLoad() throws Exception {
        String localFilePath = getClass().getClassLoader().getResource(RESOURCE_BASE + "/part-00000.avro").getPath();
        String tableName = AvroUtils.readSchemaFromLocalFile(localFilePath).getName();
        String dest = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), mainTestCustomerSpace)
                .append(tableName).append("a.avro").toString();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFilePath, dest);
        Table table = MetadataConverter.getTable(yarnConfiguration, dest);
        metadataProxy.createTable(mainTestCustomerSpace.toString(), table.getName(), table);
        Map<BusinessEntity, Table> sourceTables = new HashMap<>();
        sourceTables.put(BusinessEntity.Account, table);
        HdfsToRedshiftConfiguration exportConfig = createExportBaseConfig();
        exportConfig.setCreateNew(true);
        exportConfig.setAppend(true);
        RedshiftPublishWorkflowConfiguration.Builder builder = new RedshiftPublishWorkflowConfiguration.Builder();
        builder.hdfsToRedshiftConfiguration(exportConfig);
        builder.sourceTables(sourceTables);
        builder.enforceTargetTableName(targetTableName);
        builder.customer(mainTestCustomerSpace);
        builder.microServiceHostPort(microserviceHostPort);
        builder.internalResourceHostPort(internalResourceHostPort);
        RedshiftPublishWorkflowConfiguration config = builder.build();

        WorkflowExecutionId workflowId = workflowService.start(config);
        waitForCompletion(workflowId);
        verify(targetTableName, 122);
        verifyReport(workflowId, ReportPurpose.PUBLISH_DATA_SUMMARY, 122);
        HdfsUtils.rmdir(yarnConfiguration, dest);
    }

    @Test(groups = "workflow", dependsOnMethods = "initialLoad")
    public void updateRows() throws Exception {
        DataCollection.Version inactiveVersion = dataCollectionProxy
                .getInactiveVersion(mainTestCustomerSpace.toString());
        dataCollectionProxy.switchVersion(mainTestCustomerSpace.toString(), inactiveVersion);

        String localFilePath = getClass().getClassLoader().getResource(RESOURCE_BASE + "/part-00001.avro").getPath();
        String tableName = AvroUtils.readSchemaFromLocalFile(localFilePath).getName();
        String dest = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), mainTestCustomerSpace)
                .append(tableName).append("b.avro").toString();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFilePath, dest);
        Table table = MetadataConverter.getTable(yarnConfiguration, dest);
        Map<BusinessEntity, Table> sourceTables = new HashMap<>();
        sourceTables.put(BusinessEntity.Account, table);
        Map<BusinessEntity, Boolean> appendFlagMap = new HashMap<>();
        appendFlagMap.put(BusinessEntity.Account, true);

        HdfsToRedshiftConfiguration exportConfig = createExportBaseConfig();
        RedshiftPublishWorkflowConfiguration.Builder builder = new RedshiftPublishWorkflowConfiguration.Builder();
        builder.hdfsToRedshiftConfiguration(exportConfig);
        builder.enforceTargetTableName(targetTableName);
        builder.sourceTables(sourceTables);
        builder.appendFlagMap(appendFlagMap);
        builder.customer(mainTestCustomerSpace);
        builder.internalResourceHostPort(internalResourceHostPort);
        builder.microServiceHostPort(microserviceHostPort);
        exportConfig.setCreateNew(false);
        RedshiftPublishWorkflowConfiguration config = builder.build();
        WorkflowExecutionId workflowId = workflowService.start(config);
        waitForCompletion(workflowId);
        verify(targetTableName, 239);
        verifyReport(workflowId, ReportPurpose.PUBLISH_DATA_SUMMARY, 117);
        HdfsUtils.rmdir(yarnConfiguration, dest);
    }

    private void verifyReport(WorkflowExecutionId workflowId, ReportPurpose purpose, int count) {
        Job job = workflowJobService.getJob(CustomerSpace.parse(tenant.getId()).toString(), workflowId.getId(), true);
        Report publishDataReport = job.getReports().stream()
                .filter(r -> r.getPurpose().equals(ReportPurpose.PUBLISH_DATA_SUMMARY)).findFirst().orElse(null);
        assertNotNull(publishDataReport);
        Map<String, Integer> map = JsonUtils.deserialize(publishDataReport.getJson().getPayload(),
                new TypeReference<Map<String, Integer>>() {
                });
        assertEquals(map.get(TableRoleInCollection.BucketedAccount.name()).intValue(), count);
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }

    private void verify(String table, int size) {
        String sql = String.format("SELECT * FROM %s LIMIT 1000", table);
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        assertEquals(results.size(), size,
                "Got " + results.size() + " results in stead of " + size + " by querying [" + sql + "]");
    }
}
