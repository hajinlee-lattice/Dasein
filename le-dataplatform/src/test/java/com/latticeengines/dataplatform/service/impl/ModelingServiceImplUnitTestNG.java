package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.service.impl.ModelingServiceImpl;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

public class ModelingServiceImplUnitTestNG {

    private ModelingServiceImpl modelingService = null;
    private Algorithm algorithm = null;

    @BeforeClass(groups = "unit")
    public void setup() {
        modelingService = new ModelingServiceImpl();
        algorithm = new AlgorithmBase();
    }

    @Test(groups = "unit")
    public void doThrottlingNullConfig() {
        assertFalse(modelingService.doThrottling(null, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingDisabledConfig() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setEnabled(false);
        assertFalse(modelingService.doThrottling(config, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff2() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(2);
        assertFalse(modelingService.doThrottling(config, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff1() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(1);
        assertTrue(modelingService.doThrottling(config, algorithm, 1));
    }

    @Test(groups = "unit")
    public void getEventList() {
        List<String> targets = new ArrayList<String>();
        // Test list of column names
        targets.add("P1_Event");
        targets.add("P2_Event");
        targets.add("P2_Event");
        List<String> events = modelingService.getEventList(targets);
        assertTrue(events.size() == 2);

        // Test list of key-value pair
        targets = new ArrayList<String>();
        targets.add("Readouts: LeadID | Email");
        targets.add("Event: P1_Event");
        events = modelingService.getEventList(targets);
        assertTrue(events.size() == 1);
        assertTrue(events.contains("P1_Event"));
    }

    @Test(groups = "unit")
    public void getSortedFeatureList() {

        Map<String, Double> featureScoreMap = new HashMap<String, Double>();
        modelingService.populateFeatureScore(featureScoreMap, "column1", "0.01");
        modelingService.populateFeatureScore(featureScoreMap, "column1", "0.02");
        modelingService.populateFeatureScore(featureScoreMap, "column2", "0.03");
        modelingService.populateFeatureScore(featureScoreMap, "column2", "0.04");
        modelingService.populateFeatureScore(featureScoreMap, "column3", "0.05");
        modelingService.populateFeatureScore(featureScoreMap, "column3", "0.06");
        modelingService.populateFeatureScore(featureScoreMap, "column3", "0.07");
        assertEquals(featureScoreMap.get("column3"), 0.18d);
        assertEquals(featureScoreMap.get("column2"), 0.07d);
        assertEquals(featureScoreMap.get("column1"), 0.03d);

        List<String> features = modelingService.getSortedFeatureList(featureScoreMap, 2);
        assertEquals(features.size(), 2);
        assertEquals(features.get(0), "column3");
        assertEquals(features.get(1), "column2");

        features = modelingService.getSortedFeatureList(featureScoreMap, 3);
        assertSortedAllFeatures(features);

        features = modelingService.getSortedFeatureList(featureScoreMap, 5);
        assertSortedAllFeatures(features);

    }

    private void assertSortedAllFeatures(List<String> features) {
        assertEquals(features.size(), 3);
        assertEquals(features.get(0), "column3");
        assertEquals(features.get(1), "column2");
        assertEquals(features.get(2), "column1");
    }
}
