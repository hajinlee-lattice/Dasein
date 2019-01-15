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

import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modelreview.DataRule;

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
        assertFalse(modelingService.doThrottling(null, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingDisabledConfig() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setEnabled(false);
        assertFalse(modelingService.doThrottling(config, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff2() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(2);
        assertFalse(modelingService.doThrottling(config, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff1() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(1);
        assertTrue(modelingService.doThrottling(config,  1));
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

    @Test(groups = "unit")
    public void getReviewPipelineProps() {

        List<DataRule> dataRules = new ArrayList<>();
        Map<String, Object> propA = new HashMap<>();
        propA.put("threshold", "200");
        propA.put("limit", "10");
        DataRule ruleA = new DataRule("RuleA");
        ruleA.setEnabled(true);
        ruleA.setMandatoryRemoval(false);
        ruleA.setProperties(propA);
        dataRules.add(ruleA);

        Map<String, Object> propB = new HashMap<>();
        propB.put("Battr", "200");
        propB.put("Battr2", "10");
        DataRule ruleB = new DataRule("RuleB");
        // confirm that rule enablement does not impact review configuration
        ruleB.setEnabled(false);
        ruleB.setMandatoryRemoval(true);
        ruleB.setProperties(propB);
        dataRules.add(ruleB);

        DataRule ruleC = new DataRule("RuleC");
        ruleC.setEnabled(true);
        ruleC.setMandatoryRemoval(false);
        dataRules.add(ruleC);

        String pipelineProps = modelingService.getReviewPipelineProps(dataRules);
        Map<String, String> returnedProperties = new HashMap<>();
        String[] properties = pipelineProps.split(" ");
        String[] proptery1 = properties[0].split("=");
        String[] proptery2 = properties[1].split("=");
        String[] proptery3 = properties[2].split("=");
        String[] proptery4 = properties[3].split("=");
        returnedProperties.put(proptery1[0], proptery1[1]);
        returnedProperties.put(proptery2[0], proptery2[1]);
        returnedProperties.put(proptery3[0], proptery3[1]);
        returnedProperties.put(proptery4[0], proptery4[1]);
        assertEquals(returnedProperties.get("rulea.limit"), "\"10\"");
        assertEquals(returnedProperties.get("rulea.threshold"), "\"200\"");
        assertEquals(returnedProperties.get("ruleb.Battr"), "\"200\"");
        assertEquals(returnedProperties.get("ruleb.Battr2"), "\"10\"");
    }

}
