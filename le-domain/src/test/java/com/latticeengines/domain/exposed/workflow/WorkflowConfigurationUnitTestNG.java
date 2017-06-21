package com.latticeengines.domain.exposed.workflow;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class WorkflowConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        BaseCDLWorkflowConfiguration cdlWorkflowConfiguration = new BaseCDLWorkflowConfiguration();

        String serialized = JsonUtils.serialize(cdlWorkflowConfiguration);

        WorkflowConfiguration deserialized = JsonUtils.deserialize(serialized, WorkflowConfiguration.class);
        Assert.assertEquals(deserialized.getSwpkgName(), new BaseCDLWorkflowConfiguration().getSwpkgName());
    }


}
