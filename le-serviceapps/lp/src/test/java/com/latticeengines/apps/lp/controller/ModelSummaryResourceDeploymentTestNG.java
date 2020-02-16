package com.latticeengines.apps.lp.controller;

import static org.mockito.Mockito.when;

import javax.inject.Inject;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.impl.ModelDownloaderCallable;
import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class ModelSummaryResourceDeploymentTestNG extends LPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryResourceDeploymentTestNG.class);

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Mock
    private ModelDownloaderCallable callable;

    @BeforeClass(groups = "deployment")
    public void setup() { MockitoAnnotations.initMocks(this); }

    @Test(groups = "deployment")
    public void testDownloadModelSummary() {
        try {
            when(callable.call()).thenReturn(true);
        } catch (Exception e) {
            log.error("Failed to run model download job.", e);
        }
        boolean result = modelSummaryProxy.downloadModelSummary("LocalTest");
        Assert.assertTrue(result);
    }
}
