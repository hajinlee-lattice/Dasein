package com.latticeengines.query.evaluator.impl;

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

public class QueryProcessorTestNG extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAutowire() {
        assertNotNull(queryEvaluator);
    }
}
