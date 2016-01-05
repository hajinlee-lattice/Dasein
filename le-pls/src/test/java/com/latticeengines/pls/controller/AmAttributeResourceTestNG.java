package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.util.UriComponentsBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.ReportPurpose;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;
import com.latticeengines.pls.entitymanager.impl.ReportEntityMgrImpl;
import com.latticeengines.pls.entitymanager.impl.AmAttributeEntityMgrImpl;
import com.latticeengines.pls.entitymanager.ReportEntityMgr;

public class AmAttributeResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_ATTR_URL = "pls/amattributes/";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToExternalAdmin();
    }

    @SuppressWarnings("unchecked")
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        switchToExternalAdmin();
        List<?> reports = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports",
                List.class);
        for (Object report : reports) {
            Map<String, String> map = (Map<String, String>) report;
            restTemplate.delete(String.format("%s/pls/reports/%s", getRestAPIHostPort(), map.get("name")));
        }
        reports = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports",
                List.class);
        assertEquals(reports.size(), 0);
    }

    @Test(groups = { "functional" })
    public void findByCriterias() {

        ArrayList<Map<String, String>> paramList = new ArrayList<Map<String, String>>();
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("AttrKey", "Country");
        params.put("ParentKey", "_OBJECT_");
        params.put("ParentValue", "Account");
        paramList.add(params);
        params = new HashMap<String, String>();
        params.put("AttrKey", "Industry");
        params.put("ParentKey", "_OBJECT_");
        params.put("ParentValue", "Account");
        paramList.add(params);
        String jsonString = null;

        try {
            jsonString = new ObjectMapper().writeValueAsString(paramList);
        } catch (JsonGenerationException e) {
            assertTrue(false);
        } catch (JsonMappingException e) {
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(false);
        }

        URI attrUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_ATTR_URL)
            .queryParam("queries", jsonString) 
            .build()
            .toUri();
        System.out.println("Url Value " + attrUrl.toString());
        List attrList = restTemplate.getForObject(attrUrl, List.class);

        assertNotNull(attrList);
        assertTrue(attrList.size() == 2);

        List attrs = (List)attrList.get(0);
        validateAttrs(attrs, "Country", false);
        attrs = (List)attrList.get(1);
        validateAttrs(attrs, "Industry", false);

    }
  
    private void validateAttrs(List attrs, String key, boolean populate) {

        for (int index = 0; index < attrs.size(); index++) {
            Map map = (Map) attrs.get(index);
            assertEquals((String)map.get("AttrKey"), key);
            if (populate) {
                Map properties = (Map)map.get("Properties");
                assertTrue(properties.get("CompanyCount") != null);
            }
        }
    }

    @Test(groups = { "functional" })
    public void findWithPopulate() {

        try {
            createReports();
        } catch (Exception e) {
            assertTrue(false);
        }

        ArrayList<Map<String, String>> paramList = new ArrayList<Map<String, String>>();
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("AttrKey", "Country");
        paramList.add(params);
        params = new HashMap<String, String>();
        params.put("AttrKey", "Industry");
        params.put("ParentKey", "_OBJECT_");
        params.put("ParentValue", "Account");
        paramList.add(params);
        params = new HashMap<String, String>();
        params.put("AttrKey", "SubIndustry");
        params.put("ParentKey", "Industry");
        params.put("ParentValue", "Technology");
        paramList.add(params);

        String jsonString = null;
        try {
            jsonString = new ObjectMapper().writeValueAsString(paramList);
        } catch (JsonGenerationException e) {
            assertTrue(false);
        } catch (JsonMappingException e) {
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(false);
        }

        System.out.println("Json Value " + jsonString);
        URI attrUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_ATTR_URL)
            .queryParam("queries", jsonString)
            .queryParam("populate", "true")
            .build()
            .toUri();
        System.out.println("Url Value " + attrUrl.toString());
        List attrList = restTemplate.getForObject(attrUrl, List.class);

        assertNotNull(attrList);
        assertTrue(attrList.size() == 3);

        List attrs = (List)attrList.get(0);
        validateAttrs(attrs, "Country", true);
        attrs = (List)attrList.get(1);
        validateAttrs(attrs, "Industry", true);
        attrs = (List)attrList.get(2);
        validateAttrs(attrs, "SubIndustry", true);
    }

    private void createReports() throws Exception {
        final String json = "{ \"records\" : [ { \"value\" : \"Business Services\", \"your_customer_count\" : 12, \"in_your_db_count\" : 24 }, " +
                                             " { \"value\" : \"Consumer\", \"your_customer_count\" : 8, \"in_your_db_count\" : 36 } ] }";
        final String liftJson = "{ \"records\" : [ { \"value\" : \"Business Services\", \"lift\" : 1.678 } ] }";

        final String subIndJson = "{ \"records\" : [ { \"value\" : \"Computer Games\", \"your_customer_count\" : 12, \"in_your_db_count\" : 24 }, " +
                                             " { \"value\" : \"Computer Hardware\", \"your_customer_count\" : 8, \"in_your_db_count\" : 36 } ] }";
        final String subIndLiftJson = "{ \"records\" : [ { \"value\" : \"Computer Games\", \"lift\" : 1.678 } ] }";

        createReport("Report1", json, ReportPurpose.INDUSTRY_ATTR_LEVEL_SUMMARY); 
        createReport("Report2", liftJson, ReportPurpose.INDUSTRY_LIFT_ATTR_LEVEL_SUMMARY); 
        createReport("Report3", subIndJson, ReportPurpose.SUBINDUSTRY_ATTR_LEVEL_SUMMARY); 
        createReport("Report4", subIndLiftJson,  ReportPurpose.SUBINDUSTRY_LIFT_ATTR_LEVEL_SUMMARY); 
    }
        
    private void createReport(String name, String json, ReportPurpose purpose) throws Exception {
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        Report report = new Report();
        report.setName(name);
        report.setPurpose(purpose);
        report.setJson(kv);
        SimpleBooleanResponse rsp = restTemplate.postForObject(getRestAPIHostPort() + "/pls/reports",
                report, SimpleBooleanResponse.class);
        assertTrue(rsp.isSuccess());

    }
}
