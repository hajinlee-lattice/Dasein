package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.ScoreWorkflow;
import com.latticeengines.leadprioritization.workflow.ScoreWorkflowConfiguration;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ScoreWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {
    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private ScoreWorkflow scoreWorkflow;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    private Table accountTable;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
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

    private void setupModels() throws IOException {
        URL url = getClass().getClassLoader().getResource(RESOURCE_BASE + "/models/AccountModel");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url.getPath(),
                "/user/s-analytics/customers/" + DEMO_CUSTOMERSPACE.toString()
                        + "/models/RunMatchWithLEUniverse_152637_DerivedColumnsCache_with_std_attrib/");
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), accountTable.getName());
    }

    private ModelSummary locateModelSummary(String name, CustomerSpace space) {
        String startingHdfsPoint = "/user/s-analytics/customers/" + space;
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName().toString();
                return name.equals("modelsummary.json");
            }

        };

        try {
            List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, startingHdfsPoint, filter);
            for (String file : files) {
                String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, file);
                ModelSummary summary = modelSummaryParser.parse(file, contents);
                if (name.equals(modelSummaryParser.parseOriginalName(summary.getName()))) {
                    return summary;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    private void score(String modelId, String tableToScore) throws Exception {
        ScoreWorkflowConfiguration configuration = scoreWorkflowSubmitter.generateConfiguration(modelId, tableToScore);
        WorkflowExecutionId workflowId = workflowService.start(scoreWorkflow.name(), configuration);

        waitForCompletion(workflowId);
    }

}
