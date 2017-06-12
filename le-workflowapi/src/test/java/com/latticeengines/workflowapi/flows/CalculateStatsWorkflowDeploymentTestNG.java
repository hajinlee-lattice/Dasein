package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.CalculateStatsWorkflow;
import com.latticeengines.cdl.workflow.CalculateStatsWorkflowConfiguration;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.security.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

@Listeners({ GlobalAuthCleanupTestListener.class })
public class CalculateStatsWorkflowDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    private final Log log = LogFactory.getLog(CalculateStatsWorkflowDeploymentTestNG.class);

    @Autowired
    private CalculateStatsWorkflow calculateStatsWorkflow;

    @Autowired
    private MetadataProxy metadataProxy;

    private Table masterTable;

    protected CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("CalculateStatsWorkflowDeploymentTestNG");

    @BeforeClass(groups = "deployment")
    protected void setupForWorkflow() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        MultiTenantContext.setTenant(tenant);
        assertNotNull(MultiTenantContext.getTenant());
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);

        String hdfsPath = "/user/s-analytics/customers/" + DEMO_CUSTOMERSPACE.toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        File file = resolver.getResource("com/latticeengines/workflowapi/flows/cdl/1.avro").getFile();
        String filePath = hdfsPath + "/1.avro";
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, file.getAbsolutePath(), filePath);
        System.out.println("filePath is " + filePath);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, filePath);
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, filePath);

        // TODO directly use AvroUtils to write to HDFS.

        File f = new File("/tmp/tmp.avro");
        FileUtils.touch(f);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>())) {
            writer.create(schema, f);
            int count = 0;
            while (iterator.hasNext()) {
                count++;
                GenericRecord record = iterator.next();
                writer.append(record);
                if (count == 6) {
                    break;
                }
            }
        }
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, f.getAbsolutePath(), hdfsPath + "/tmp.avro");

        List<GenericRecord> list = AvroUtils.getData(yarnConfiguration, new Path(hdfsPath + "/tmp.avro"));
        System.out.println("list is " + Arrays.toString(list.toArray()));
        System.out.println("list size is " + list.size());

        Table table = MetadataConverter.getTable(yarnConfiguration, filePath);
        metadataProxy.createTable(DEMO_CUSTOMERSPACE.toString(), table.getName(), table);

        Table retrievedTable = metadataProxy.getTable(DEMO_CUSTOMERSPACE.toString(), table.getName());
        Assert.assertNotNull(retrievedTable);
        System.out.println("attributes are" + Arrays.toString(retrievedTable.getAttributes().toArray()));
        masterTable = retrievedTable;
    }

    @AfterClass(groups = "deployment")
    protected void cleanUpAfterWorkflow() throws Exception {
        deleteTenantByRestCall(DEMO_CUSTOMERSPACE.toString());
        cleanCamille(DEMO_CUSTOMERSPACE);
        cleanHdfs(DEMO_CUSTOMERSPACE);
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflow() throws Exception {

        log.info("customer is " + DEMO_CUSTOMERSPACE.getTenantId());
        CalculateStatsWorkflowConfiguration config = generateConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(calculateStatsWorkflow.name(), config);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        Assert.assertEquals(status, BatchStatus.COMPLETED);
    }

    private CalculateStatsWorkflowConfiguration generateConfiguration() {
        Assert.assertNotNull(masterTable.getName());
        System.out.println("masterTable name is " + masterTable.getName());
        return new CalculateStatsWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(DEMO_CUSTOMERSPACE.getTenantId())) //
                .masterTableName(masterTable.getName()).build();
    }

}
