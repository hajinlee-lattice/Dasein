package com.latticeengines.ulysses.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;
import com.latticeengines.ulysses.service.EnrichedAccountService;

public class EnrichedAccountServiceImplTestNG extends UlyssesTestNGBase {

    @Autowired
    private EnrichedAccountService scoreAndEnrichmentService;

    @Test(groups = "functional")
    public void init() {
    }
}
