package com.latticeengines.dataplatform.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class YarnQueueAssignmentServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private YarnQueueAssignmentServiceImpl yarnQueueAssignmentService;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @Test(groups = "functional")
    public void updateCurrentQueueState() {
        yarnQueueAssignmentService.updateCurrentQueueState();
        Map<Integer, String> map = yarnQueueAssignmentService.getFullyQualifiedQueueNames();
        
    }
}
