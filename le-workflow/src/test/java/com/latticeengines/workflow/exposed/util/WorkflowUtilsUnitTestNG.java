package com.latticeengines.workflow.exposed.util;

import java.io.InputStream;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class WorkflowUtilsUnitTestNG {

    @Test(groups = { "unit" })
    public void test() throws ClassNotFoundException {
        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/workflow/config/workflow.conf");
        WorkflowConfiguration workflowConfig = JsonUtils.deserialize(is, WorkflowConfiguration.class);
        System.out.println(WorkflowUtils.getFlattenedConfig(workflowConfig));
    }
}
