package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.Predictor;

public class ModelSummaryEntityMgrImplUnitTestNG {

    private ModelSummaryEntityMgrImpl modelSummaryEntityMgrImpl;
    private List<Predictor> predictorList;
    private AttributeMap attrMap1;
    private AttributeMap attrMap2;

    @BeforeClass(groups = "unit")
    public void setup() {
        modelSummaryEntityMgrImpl = new ModelSummaryEntityMgrImpl();

        predictorList = new ArrayList<Predictor>();
        Predictor predictor1 = new Predictor();
        predictor1.setName("p1");
        Predictor predictor2 = new Predictor();
        predictor2.setName("p2");
        predictorList.add(predictor1);
        predictorList.add(predictor2);
        attrMap1 = new AttributeMap();
        attrMap1.put("p1", "1");
        attrMap1.put("p2", "1");
        attrMap2 = new AttributeMap();
        attrMap2.put("p1", "1");
        attrMap2.put("p2", "1");
        attrMap2.put("p3", "0");
        attrMap2.put("p4", "0");
    }

    @Test(groups = "unit", enabled = true)
    public void testGetMissingPredictors() {
        List<String> missingPredictors = null;
        missingPredictors = modelSummaryEntityMgrImpl.getMissingPredictors(predictorList, attrMap1);
        assertTrue(missingPredictors.size() == 0);
        missingPredictors = modelSummaryEntityMgrImpl.getMissingPredictors(predictorList, attrMap2);
        assertTrue(missingPredictors.size() == 2);
        try {
            missingPredictors = modelSummaryEntityMgrImpl.getMissingPredictors(null, attrMap2);
            assertTrue(true, "Should have thrown exception");
        } catch (Exception e) {
            assertTrue(e instanceof NullPointerException);
        }
        try {
            missingPredictors = modelSummaryEntityMgrImpl.getMissingPredictors(predictorList, null);
            assertTrue(true, "Should have thrown exception");
        } catch (Exception e) {
            assertTrue(e instanceof NullPointerException);
        }
    }

}
