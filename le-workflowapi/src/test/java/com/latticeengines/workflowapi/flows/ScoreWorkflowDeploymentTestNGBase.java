package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ScoreWorkflowDeploymentTestNGBase extends ImportMatchAndModelWorkflowDeploymentTestNGBase {
    static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    private Table accountTable;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.setup();
        setupTables();
        setupModels();
    }

    private void setupTables() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "/tables/Account.json");
        accountTable = JsonUtils.deserialize(IOUtils.toString(ins), Table.class);
        accountTable.setName("ScoreWorkflowDeploymentTest_Account");
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), accountTable.getName(),
                accountTable);

        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/tables/Account.avro");
        String parent = accountTable.getExtracts().get(0).getPath().replace("*.avro", "Account.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(), parent);
    }

    void setupModels() throws IOException {
        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/models/AccountModel");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(),
                "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                        + "/models/RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib/");
        URL eventtableDatacompositionUrl = getClass().getClassLoader().getResource(RESOURCE_BASE
                + "/models/AccountModel/20a331e9-f18b-4358-8023-e44a36cb17dd/1459178858615_0234/enhancements/datacomposition.json");
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, eventtableDatacompositionUrl.getPath(),
                "/user/s-analytics/customers/" + mainTestCustomerSpace.toString()
                        + "/data/RunMatchWithLEUniverse_152722_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json");
    }

    @Test(groups = "deployment", enabled = false)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", mainTestCustomerSpace);
        assertNotNull(summary);
        score(summary.getId(), accountTable.getName());
    }

    private void score(String modelId, String tableToScore) throws Exception {
        ScoreWorkflowConfiguration configuration = scoreWorkflowSubmitter.generateConfiguration(modelId, tableToScore,
                new Table(), tableToScore, TransformationGroup.STANDARD);
        WorkflowExecutionId workflowId = workflowService.start(configuration);

        waitForCompletion(workflowId);
    }

}
