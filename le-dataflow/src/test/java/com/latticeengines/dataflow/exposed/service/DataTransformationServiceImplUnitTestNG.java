package com.latticeengines.dataflow.exposed.service;

import org.testng.annotations.Test;

import com.latticeengines.dataflow.service.impl.DataTransformationServiceImpl;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpException;

public class DataTransformationServiceImplUnitTestNG {

    @Test(groups = "unit", expectedExceptions = LedpException.class)
    public void validateParameters() {
        new DataTransformationServiceImpl().executeNamedTransformation(new DataFlowContext(), "doesntMatterShouldThrowException");
    }
}
