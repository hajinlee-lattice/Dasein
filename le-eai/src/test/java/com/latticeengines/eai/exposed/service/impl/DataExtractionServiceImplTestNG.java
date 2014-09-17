package com.latticeengines.eai.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

public class DataExtractionServiceImplTestNG extends EaiFunctionalTestNGBase {
    
    @Autowired
    private DataExtractionServiceImpl dataExtractionService;

    @Test(groups = "functional")
    public void importData() throws Exception {
        dataExtractionService.importData();
        Thread.sleep(30000L);
    }
}
