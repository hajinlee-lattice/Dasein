package com.latticeengines.dataflow.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;

public class DataTransformationServiceImplTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private DataTransformationServiceImpl dataTransformationService;

    @Test(groups = "functional")
    public void executeNamedTransformation() {
        dataTransformationService.executeNamedTransformation(null, "sampleDataFlowBuilder");
    }
}
