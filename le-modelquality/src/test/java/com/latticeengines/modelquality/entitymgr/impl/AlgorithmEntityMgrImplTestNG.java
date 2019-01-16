package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class AlgorithmEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private Algorithm algorithm;
    private final String algorithmName = "AlgorithmEntityMgrImplTestNG";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        Algorithm alreadyExists = algorithmEntityMgr.findByName(algorithmName);
        if (alreadyExists != null)
            algorithmEntityMgr.delete(alreadyExists);

        algorithm = new Algorithm();
        algorithm.setName(algorithmName);
        algorithm.setScript("/datascience/dataplatform/scripts/random_forest.py");
        AlgorithmPropertyDef numTrees = new AlgorithmPropertyDef("n_estimators");
        AlgorithmPropertyValue numTrees100 = new AlgorithmPropertyValue("100");
        AlgorithmPropertyValue numTrees200 = new AlgorithmPropertyValue("200");
        AlgorithmPropertyValue numTrees300 = new AlgorithmPropertyValue("300");
        algorithm.addAlgorithmPropertyDef(numTrees);
        numTrees.addAlgorithmPropertyValue(numTrees100);
        numTrees.addAlgorithmPropertyValue(numTrees200);
        numTrees.addAlgorithmPropertyValue(numTrees300);
    }

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        algorithmEntityMgr.delete(algorithm);
        super.tearDown();
    }

    @Test(groups = "functional")
    public void create() {
        algorithmEntityMgr.create(algorithm);

        List<Algorithm> retrievedAlgorithms = algorithmEntityMgr.findAll();
        assertNotNull(retrievedAlgorithms);

        Algorithm retrievedAlgorithm = algorithmEntityMgr.findByName(algorithmName);
        assertEquals(retrievedAlgorithm.getName(), algorithm.getName());
        assertEquals(retrievedAlgorithm.getScript(), algorithm.getScript());

        List<AlgorithmPropertyDef> retrievedPropertyDefs = algorithm.getAlgorithmPropertyDefs();
        assertEquals(retrievedPropertyDefs.size(), 1);
        AlgorithmPropertyDef retrievedPropertyDef = retrievedPropertyDefs.get(0);

        List<AlgorithmPropertyValue> retrievedPropertyValues = retrievedPropertyDef.getAlgorithmPropertyValues();
        assertEquals(retrievedPropertyValues.size(), 3);
    }
}
