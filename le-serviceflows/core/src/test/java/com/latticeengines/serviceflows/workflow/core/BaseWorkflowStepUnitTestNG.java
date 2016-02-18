package com.latticeengines.serviceflows.workflow.core;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class BaseWorkflowStepUnitTestNG {

    // have to do this since BaseWorkflowStep is abstract
    class Step extends BaseWorkflowStep<BaseStepConfiguration> {

        @Override
        public void execute() {
        }

    }
    private Step step = new Step();

    @Test(groups = "unit", dataProvider = "paths")
    public void getHdfsDir(String path, String expectedPath) {
        String resultPath = step.getHdfsDir(path);
        assertEquals(resultPath, expectedPath);
    }

    @DataProvider(name = "paths")
    public Object[][] getPaths() {
        return new Object[][] {
                { "/tmp/a.avro", "/tmp" }, //
                { "/tmp/xyz/", "/tmp/xyz" }, //
                { "/tmp/*.avro", "/tmp" } //
        };
    }
}
