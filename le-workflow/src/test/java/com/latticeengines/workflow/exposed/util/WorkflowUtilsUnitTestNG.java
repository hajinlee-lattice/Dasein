package com.latticeengines.workflow.exposed.util;

import java.io.InputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class WorkflowUtilsUnitTestNG {

    @Test(groups = {"unit"})
    public void test() {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/workflow/config/workflow.conf");
        WorkflowConfiguration workflowConfig = JsonUtils.deserialize(is, WorkflowConfiguration.class);
        System.out.println(WorkflowUtils.getFlattenedConfig(workflowConfig));
        LedpCode ledpCode = WorkflowUtils.getWorkFlowErrorCode("Application application_1559836857064_15467 failed 1 times due to AM " +
                "Container for appattempt_1559836857064_15467_000001 exited with exitCode: -100\n" +
                "Failing this attempt.Diagnostics: Container released on a *lost* nodeFor more detailed output, check the application tracking page: http://ip-10-141-111-172.lattice.local:8188/applicationhistory/app/application_1559836857064_15467 Then click on links to logs of each attempt.\n" +
                ". Failing the application.");
        Assert.assertEquals(ledpCode, LedpCode.LEDP_28031);
        Assert.assertEquals(WorkflowUtils.getWorkFlowErrorCode("Application failed"), LedpCode.LEDP_28015);
    }
}
