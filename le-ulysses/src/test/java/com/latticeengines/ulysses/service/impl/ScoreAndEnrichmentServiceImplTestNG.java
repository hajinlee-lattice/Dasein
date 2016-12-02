package com.latticeengines.ulysses.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;
import com.latticeengines.ulysses.service.ScoreAndEnrichmentService;

public class ScoreAndEnrichmentServiceImplTestNG extends UlyssesTestNGBase {

    @Autowired
    private ScoreAndEnrichmentService scoreAndEnrichmentService;

    @Test(groups = "functional")
    public void init() {
    }
}
