package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryParserTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Test(groups = "functional")
    public void parse() {
        throw new RuntimeException("Test not implemented");
    }

}
