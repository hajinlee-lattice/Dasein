package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

@ContextConfiguration(locations = {"classpath:pls-modeldownload-quartz-context.xml"})
public class ModelDownloaderMBeanTestNG extends PlsFunctionalTestNGBaseDeprecated {

	@Autowired
	private ModelDownloaderMBean modelDownloaderMBean;

	@Test(groups = {"functional", "functional.production"})
    public void testCheckQuartzJob() {
        assertTrue(modelDownloaderMBean.checkModelDownloader().contains("SUCCESS"));
    }
}
