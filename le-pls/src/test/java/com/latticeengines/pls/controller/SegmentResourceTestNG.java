package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class SegmentResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
        restTemplate.setErrorHandler(statusErrorHandler);
    }
    
    @Test(groups = { "functional", "deployment" })
    public void delete() {
        restTemplate.delete(getRestAPIHostPort() + "/pls/segments/SMB", new HashMap<>());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "delete" })
    public void createSegmentHasEditPlsModelsRight() {
        List summaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        Map<String, String> map = (Map) summaries.get(0);
        Segment segment = new Segment();
        segment.setName("EMEA");
        segment.setPriority(3);
        segment.setModelId(map.get("Id"));
        ResponseDocument response = restTemplate.postForObject(getRestAPIHostPort() + "/pls/segments/", segment,
                ResponseDocument.class, new HashMap<>());
        assertTrue(response.isSuccess());
    }

    @Test(groups = { "functional", "deployment" }, enabled = false)
    public void createSegmentNoEditPlsModelsRight() {
        switchToExternalUser();
    }

    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "createSegmentHasEditPlsModelsRight" }, enabled = false)
    public void getSegmentByName() {
        Segment segment = restTemplate.getForObject(getRestAPIHostPort() + "/pls/segments/EMEA", Segment.class);
        assertNotNull(segment);
        assertEquals(segment.getPriority().intValue(), 3);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "createSegmentHasEditPlsModelsRight" }, enabled = false)
    public void getSegments() {
        List segments = restTemplate.getForObject(getRestAPIHostPort() + "/pls/segments/", List.class);
        assertEquals(segments.size(), 1);
    }

    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "getSegmentByName" }, enabled = false)
    public void update() {
        Segment segment = restTemplate.getForObject(getRestAPIHostPort() + "/pls/segments/EMEA", Segment.class);
        segment.setPriority(2);
        restTemplate.put(getRestAPIHostPort() + "/pls/segments/EMEA", segment);
        segment = restTemplate.getForObject(getRestAPIHostPort() + "/pls/segments/EMEA", Segment.class);
        assertNotNull(segment);
        assertEquals(segment.getPriority().intValue(), 2);
        
    }
}
