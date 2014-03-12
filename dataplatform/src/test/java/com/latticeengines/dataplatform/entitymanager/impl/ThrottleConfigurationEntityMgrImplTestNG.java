package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class ThrottleConfigurationEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    @Autowired
    private ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr;
    
    private ThrottleConfiguration config;
    
    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }
    
    @BeforeClass(groups = "functional")
    public void setup() {
        throttleConfigurationEntityMgr.deleteStoreFile();
        config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        config.setTimestamp(System.currentTimeMillis());
    }

    @Test(groups = "functional")
    public void postThenSave() {
        throttleConfigurationEntityMgr.post(config);
        ThrottleConfiguration retrievedConfig = throttleConfigurationEntityMgr.getById(config.getId());
        assertEquals(Integer.valueOf(5), retrievedConfig.getJobRankCutoff());
        assertTrue(retrievedConfig.isImmediate());
        assertEquals(config.getTimestamp(), retrievedConfig.getTimestamp());
        throttleConfigurationEntityMgr.save();
    }

}
