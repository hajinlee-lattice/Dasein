package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.pls.service.impl.ModelSummaryParser;

public class ModelAndScoreDeploymentTestNG extends ModelAndScoreDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    private SourceFile accountSourceFile;
    private SourceFile leadSourceFile;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
    }

    @Test(groups = "deployment", enabled = true)
    public void modelAccount() throws Exception {
        accountSourceFile = uploadFile(RESOURCE_BASE + "/Account.csv", SchemaInterpretation.SalesforceAccount);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(accountSourceFile.getName());
        params.setName("testWorkflowAccount");
        model(params);
    }

    @Test(groups = "deployment", enabled = false)
    public void scoreAccount() throws Exception {
        accountSourceFile = internalResourceProxy.findSourceFileByName("Account.csv", DEMO_CUSTOMERSPACE.toString());
        assertEquals(accountSourceFile.getState(), SourceFileState.Imported);
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), accountSourceFile.getTableName());
    }

    @Test(groups = "deployment", enabled = true)
    public void modelLead() throws Exception {
        leadSourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.SalesforceLead);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(leadSourceFile.getName());
        params.setName("testWorkflowLead");
        model(params);
    }

    @Test(groups = "deployment", dependsOnMethods = "modelLead", enabled = false)
    public void scoreLead() throws Exception {
        leadSourceFile = internalResourceProxy.findSourceFileByName(leadSourceFile.getName(),
                DEMO_CUSTOMERSPACE.toString());
        assertEquals(leadSourceFile.getState(), SourceFileState.Imported);
        ModelSummary summary = locateModelSummary("testWorkflowLead", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), leadSourceFile.getTableName());
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
}
