package com.latticeengines.apps.lp.controller;

import static org.mockito.Mockito.when;

import javax.inject.Inject;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.impl.ModelDownloaderCallable;
import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class ModelSummaryResourceTestNG extends LPDeploymentTestNGBase {

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
            e.printStackTrace();
        }

        boolean result = modelSummaryProxy.downloadModelSummary("LocalTest");
        Assert.assertTrue(result);
    }
}
