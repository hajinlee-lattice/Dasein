package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;

public class ModelEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    @Autowired
    private ModelEntityMgrImpl modelEntityMgr;
    
    private Model model;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }
    
    @BeforeClass(groups = "functional")
    public void setup() {
        modelEntityMgr.deleteStoreFile();
        
        Job job1 = new Job();
        job1.setId("application_12345_00001");
        Job job2 = new Job();
        job2.setId("application_12345_00002");
        
        model = new Model();
        model.addJob(job1);
        model.addJob(job2);
    }
    
    private void verifyRetrievedModel(String id) {
        Model retrievedModel = modelEntityMgr.getById(model.getId());
        assertNotNull(retrievedModel);
        assertEquals(model.getId(), retrievedModel.getId());
        assertEquals(2, model.getJobs().size());
    }

    @Test(groups = "functional")
    public void postThenSave() {
        modelEntityMgr.post(model);
        verifyRetrievedModel(model.getId());
        modelEntityMgr.save();
    }
    
    @Test(groups = "functional", dependsOnMethods = { "postThenSave" })
    public void clear() {
        modelEntityMgr.clear();
        assertNull(modelEntityMgr.getById(model.getId()));
    }

    @Test(groups = "functional", dependsOnMethods = { "clear" })
    public void load() {
        modelEntityMgr.load();
        verifyRetrievedModel(model.getId());
    }
}
