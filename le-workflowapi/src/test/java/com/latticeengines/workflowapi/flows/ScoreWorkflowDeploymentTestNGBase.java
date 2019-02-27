package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceflows.scoring.ScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class ScoreWorkflowDeploymentTestNGBase extends ImportMatchAndModelWorkflowDeploymentTestNGBase {
    static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    private Table accountTable;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
        setupTables();
        setupModels();
    }

    private void setupTables() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "/tables/Account.json");
        accountTable = JsonUtils.deserialize(IOUtils.toString(ins, "UTF-8"), Table.class);
        accountTable.setName("ScoreWorkflowDeploymentTest_Account");
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), accountTable.getName(),
                accountTable);

        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/Account.avro");
        String parent = accountTable.getExtracts().get(0).getPath().replace("*.avro", "Account.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(), parent);
    }

    void setupModels() throws IOException {
        String uuid = UUID.randomUUID().toString();
        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/models/AccountModel/random_uuid");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(),
                "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                        + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/" + uuid);
        String summaryHdfsPath = "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/" + uuid
                + "/enhancements/modelsummary.json";
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, summaryHdfsPath));

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(RESOURCE_BASE + "/models/AccountModel/random_uuid/enhancements/modelsummary.json");
        String summary = IOUtils.toString(is, Charset.forName("UTF-8"));
        summary = summary.replace("{% uuid %}", uuid);
        HdfsUtils.writeToFile(yarnConfiguration, summaryHdfsPath, summary);
        URL eventtableDatacompositionUrl = getClass().getClassLoader().getResource(RESOURCE_BASE
                + "/models/AccountModel/20a331e9-f18b-4358-8023-e44a36cb17dd/1459178858615_0234/enhancements/datacomposition.json");
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, eventtableDatacompositionUrl.getPath(),
                "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                        + "/data/RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json");
    }

    @Test(groups = "workflow", enabled = false)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", mainTestCustomerSpace);
        assertNotNull(summary);
        score(summary.getId(), accountTable.getName());
    }

    private void score(String modelId, String tableToScore) throws Exception {
        ScoreWorkflowConfiguration workflowConfig = scoreWorkflowSubmitter.generateConfiguration(modelId, tableToScore,
                new Table(), tableToScore, TransformationGroup.STANDARD);

        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);

        waitForCompletion(workflowId);
    }

}
