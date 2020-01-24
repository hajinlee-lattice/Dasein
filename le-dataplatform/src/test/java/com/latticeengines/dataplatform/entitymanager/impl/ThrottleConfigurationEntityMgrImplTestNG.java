package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Calendar;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
public class ThrottleConfigurationEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    private ThrottleConfiguration config;

    @Inject
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @BeforeClass(groups = {"functional"})
    public void setup() {
        config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        config.setTimestampLong(System.currentTimeMillis());
    }

    @Test(groups = {"functional"})
    public void testPersist() {
        throttleConfigurationEntityMgr.create(config);
    }

    @Test(groups = {"functional"}, dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        ThrottleConfiguration newConfig = new ThrottleConfiguration(config.getPid());
        newConfig = throttleConfigurationEntityMgr.findByKey(newConfig); // /
                                                                         // getByKey(newConfig);

        newConfig.getTimestamp();
        Calendar configTime = Calendar.getInstance();
        Calendar newConfigTime = Calendar.getInstance();
        configTime.setTimeInMillis(config.getTimestamp().getTime());
        newConfigTime.setTimeInMillis(newConfig.getTimestamp().getTime());
        // workaround for nano-second discrepency
        boolean assertion = (configTime.get(Calendar.YEAR) == newConfigTime.get(Calendar.YEAR))
                && (configTime.get(Calendar.MONTH) == newConfigTime.get(Calendar.MONTH))
                && (configTime.get(Calendar.DAY_OF_MONTH) == newConfigTime.get(Calendar.DAY_OF_MONTH))
                && (configTime.get(Calendar.HOUR) == newConfigTime.get(Calendar.HOUR))
                && (configTime.get(Calendar.MINUTE) == newConfigTime.get(Calendar.MINUTE));
        assertEquals(assertion, true);
        assertEquals(newConfig.getJobRankCutoff(), config.getJobRankCutoff());
        assertEquals(newConfig.isEnabled(), config.isEnabled());
        assertEquals(newConfig.isImmediate(), config.isImmediate());
    }

    @Test(groups = {"functional"}, dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(config.getPid());
        config.setEnabled(true);
        config.setJobRankCutoff(10);
        config.setImmediate(false);
        throttleConfigurationEntityMgr.update(config);

        testRetrieval();
    }

    @Test(groups = {"functional"}, dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        ThrottleConfiguration newConfig = new ThrottleConfiguration(config.getPid());
        newConfig = throttleConfigurationEntityMgr.findByKey(newConfig);

        assertNotNull(newConfig.getTimestamp());
        throttleConfigurationEntityMgr.delete(newConfig);
        newConfig = null;
        newConfig = throttleConfigurationEntityMgr.findByKey(config);
        assertNull(newConfig);
    }

}
