package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
public class ModelDownloaderMBeanNoDownloaderQuartzRunningTestNG
        extends PlsFunctionalTestNGBaseDeprecated {

    @Inject
    private ModelDownloaderMBean modelDownloaderMBean;

    @Test(groups = { "functional", "functional.production" })
    public void testCheckQuartzJob() {
        assertTrue(modelDownloaderMBean.checkModelDownloader().contains("FAILURE"));
    }
}
