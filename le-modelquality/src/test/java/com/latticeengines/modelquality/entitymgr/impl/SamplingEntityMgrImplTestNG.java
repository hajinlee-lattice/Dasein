package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class SamplingEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    private Sampling sampling;
    
    @Autowired
    private SamplingEntityMgr samplingEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        samplingEntityMgr.deleteAll();
        
        sampling = new Sampling();
        sampling.setName("Random Forest");
        SamplingPropertyDef numTrees = new SamplingPropertyDef("samplingRate");
        SamplingPropertyValue sampling80 = new SamplingPropertyValue("80");
        SamplingPropertyValue sampling30 = new SamplingPropertyValue("30");
        SamplingPropertyValue sampling100 = new SamplingPropertyValue("100");
        sampling.addSamplingPropertyDef(numTrees);
        numTrees.addSamplingPropertyValue(sampling80);
        numTrees.addSamplingPropertyValue(sampling30);
        numTrees.addSamplingPropertyValue(sampling100);
    }

    @Test(groups = "functional")
    public void create() {
        samplingEntityMgr.create(sampling);
        
        List<Sampling> retrievedSamplings = samplingEntityMgr.findAll();
        assertEquals(retrievedSamplings.size(), 1);
        Sampling retrievedSampling = retrievedSamplings.get(0);
        
        assertEquals(retrievedSampling.getName(), sampling.getName());
        
        List<SamplingPropertyDef> retrievedPropertyDefs = sampling.getSamplingPropertyDefs();
        assertEquals(retrievedPropertyDefs.size(), 1);
        SamplingPropertyDef retrievedPropertyDef = retrievedPropertyDefs.get(0);
        
        List<SamplingPropertyValue> retrievedPropertyValues = retrievedPropertyDef.getSamplingPropertyValues();
        assertEquals(retrievedPropertyValues.size(), 3);
    }
}
