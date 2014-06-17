package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.sql.Timestamp;

import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional  
public class ThrottleConfigurationEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    private ThrottleConfiguration config;
    
    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }
    
    @BeforeClass(groups = "functional")
    public void setup() {
        config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        config.setTimestampLong(System.currentTimeMillis());
    }

    @Transactional
    @Test(groups = "functional")
    public void testPersist() {    	
    	throttleConfigurationEntityMgr.create(config);
    }
    
    @Transactional
    @Test(groups = "functional", dependsOnMethods={"testPersist"})
    public void testRetrieval() {
    	ThrottleConfiguration newConfig = new ThrottleConfiguration(config.getPid());    	
    	newConfig = throttleConfigurationEntityMgr.findByKey(newConfig);  ///  getByKey(newConfig);
    	
    	Timestamp ts1 = config.getTimestamp();
    	Timestamp newTs = newConfig.getTimestamp();
    	// workaround for nano-second discrepency
    	boolean assertion = (ts1.getYear()==newTs.getYear()) && (ts1.getMonth()==newTs.getMonth()) && (ts1.getDay()==newTs.getDay()) && (ts1.getHours()==newTs.getHours()) && (ts1.getMinutes()==newTs.getMinutes());                     
    	
    	assertEquals(assertion, true);
    	assertEquals(newConfig.getJobRankCutoff(), config.getJobRankCutoff());    	
    	assertEquals(newConfig.isEnabled(), config.isEnabled());
    	assertEquals(newConfig.isImmediate(), config.isImmediate());
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods={"testPersist"})
    public void testUpdate() {    	
        assertNotNull(config.getPid());
    	config.setEnabled(true);
    	config.setJobRankCutoff(10);
    	config.setImmediate(false);
    	throttleConfigurationEntityMgr.update(config);
    	
    	testRetrieval();
    }
    
    @Transactional
    @Test(groups = "functional", dependsOnMethods={"testUpdate"})
    public void testDelete() {
    	ThrottleConfiguration newConfig = new ThrottleConfiguration(config.getPid());    	
    	newConfig = throttleConfigurationEntityMgr.findByKey(newConfig);  ///getByKey(newConfig);
      
    	assertNotNull(newConfig.getTimestamp());
        throttleConfigurationEntityMgr.delete(newConfig);    	
    	// should be deleted
        newConfig = null;
        newConfig = throttleConfigurationEntityMgr.findByKey(config);  ///getByKey(config);
        assertNull(newConfig);
    }
    
    // TODO REMOVE
    public void postThenSave() {
        //throttleConfigurationEntityMgr.post(config);
        ThrottleConfiguration retrievedConfig = throttleConfigurationEntityMgr.findByKey(new ThrottleConfiguration(config.getPid()));  ///getById(config.getPid());
        assertEquals(Integer.valueOf(5), retrievedConfig.getJobRankCutoff());
        assertTrue(retrievedConfig.isImmediate());
        assertEquals(config.getTimestamp(), retrievedConfig.getTimestamp());
        //throttleConfigurationEntityMgr.save();
    }

}
