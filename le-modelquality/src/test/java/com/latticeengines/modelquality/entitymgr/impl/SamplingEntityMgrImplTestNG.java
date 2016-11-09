package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class SamplingEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private Sampling sampling;
    private final String samplingName = "SamplingEntityMgrImplTestNG";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        Sampling alreadyExists = samplingEntityMgr.findByName(samplingName);
        if (alreadyExists != null)
            samplingEntityMgr.delete(alreadyExists);
        sampling = new Sampling();
        sampling.setName(samplingName);
        SamplingPropertyDef numTrees = new SamplingPropertyDef("samplingRate");
        SamplingPropertyValue sampling80 = new SamplingPropertyValue("80");
        SamplingPropertyValue sampling30 = new SamplingPropertyValue("30");
        SamplingPropertyValue sampling100 = new SamplingPropertyValue("100");
        sampling.addSamplingPropertyDef(numTrees);
        numTrees.addSamplingPropertyValue(sampling80);
        numTrees.addSamplingPropertyValue(sampling30);
        numTrees.addSamplingPropertyValue(sampling100);
    }

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        samplingEntityMgr.delete(sampling);
        super.tearDown();
    }

    @Test(groups = "functional")
    public void create() {
        samplingEntityMgr.create(sampling);

        List<Sampling> retrievedSamplings = samplingEntityMgr.findAll();
        Sampling retrievedSampling = samplingEntityMgr.findByName("SamplingEntityMgrImplTestNG");

        assertEquals(retrievedSampling.getName(), sampling.getName());

        List<SamplingPropertyDef> retrievedPropertyDefs = sampling.getSamplingPropertyDefs();
        assertEquals(retrievedPropertyDefs.size(), 1);
        SamplingPropertyDef retrievedPropertyDef = retrievedPropertyDefs.get(0);

        List<SamplingPropertyValue> retrievedPropertyValues = retrievedPropertyDef.getSamplingPropertyValues();
        assertEquals(retrievedPropertyValues.size(), 3);
    }
}
