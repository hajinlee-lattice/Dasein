package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelDownloaderMBeanNoDownloaderQuartzRunningTestNG extends PlsFunctionalTestNGBase {

	@Autowired
	private ModelDownloaderMBean modelDownloaderMBean;

	@Test(groups = {"functional", "functional.production"})
    public void testCheckQuartzJob() {
        assertTrue(modelDownloaderMBean.checkModelDownloader().contains("FAILURE"));
    }
}
