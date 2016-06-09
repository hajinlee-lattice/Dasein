package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class AlgorithmEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    private Algorithm algorithm;
    
    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        algorithmEntityMgr.deleteAll();
        
        algorithm = new Algorithm();
        algorithm.setName("Random Forest");
        algorithm.setScript("/app/dataplatform/scripts/random_forest.py");
        AlgorithmPropertyDef numTrees = new AlgorithmPropertyDef("n_estimators");
        AlgorithmPropertyValue numTrees100 = new AlgorithmPropertyValue("100");
        AlgorithmPropertyValue numTrees200 = new AlgorithmPropertyValue("200");
        AlgorithmPropertyValue numTrees300 = new AlgorithmPropertyValue("300");
        algorithm.addAlgorithmPropertyDef(numTrees);
        numTrees.addAlgorithmPropertyValue(numTrees100);
        numTrees.addAlgorithmPropertyValue(numTrees200);
        numTrees.addAlgorithmPropertyValue(numTrees300);
    }

    @Test(groups = "functional")
    public void create() {
        algorithmEntityMgr.create(algorithm);
        
        List<Algorithm> retrievedAlgorithms = algorithmEntityMgr.findAll();
        assertEquals(retrievedAlgorithms.size(), 1);
        Algorithm retrievedAlgorithm = retrievedAlgorithms.get(0);
        
        assertEquals(retrievedAlgorithm.getName(), algorithm.getName());
        assertEquals(retrievedAlgorithm.getScript(), algorithm.getScript());
        
        List<AlgorithmPropertyDef> retrievedPropertyDefs = algorithm.getAlgorithmPropertyDefs();
        assertEquals(retrievedPropertyDefs.size(), 1);
        AlgorithmPropertyDef retrievedPropertyDef = retrievedPropertyDefs.get(0);
        
        List<AlgorithmPropertyValue> retrievedPropertyValues = retrievedPropertyDef.getAlgorithmPropertyValues();
        assertEquals(retrievedPropertyValues.size(), 3);
    }
}
