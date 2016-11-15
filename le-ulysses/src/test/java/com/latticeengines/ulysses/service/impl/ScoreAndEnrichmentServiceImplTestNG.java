package com.latticeengines.ulysses.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.ulysses.functionalframework.UlyssesFunctionalTestNGBase;
import com.latticeengines.ulysses.service.ScoreAndEnrichmentService;

public class ScoreAndEnrichmentServiceImplTestNG extends UlyssesFunctionalTestNGBase {
    
    @Autowired
    private ScoreAndEnrichmentService scoreAndEnrichmentService;

    @Test(groups = "functional")
    public void init() {
    }
}
