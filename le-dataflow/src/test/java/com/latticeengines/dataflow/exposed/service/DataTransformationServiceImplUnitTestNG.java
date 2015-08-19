package com.latticeengines.dataflow.exposed.service;

import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.exception.DataFlowException;
import com.latticeengines.dataflow.service.impl.DataTransformationServiceImpl;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class DataTransformationServiceImplUnitTestNG {

    @Test(groups = "unit", expectedExceptions = DataFlowException.class)
    public void validateParameters() {
        new DataTransformationServiceImpl().executeNamedTransformation(new DataFlowContext(), "doesntMatterShouldThrowException");
    }
}
