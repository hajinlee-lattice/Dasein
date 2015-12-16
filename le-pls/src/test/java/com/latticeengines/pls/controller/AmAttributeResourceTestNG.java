package com.latticeengines.pls.controller;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.net.URI;
import java.io.IOException;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class AmAttributeResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_ATTR_URL = "pls/amattributes/";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
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
            assertEquals((String)map.get("ParentKey"), "_OBJECT_");
            assertEquals((String)map.get("ParentValue"), "Account");
            if (populate) {
                Map properties = (Map)map.get("Properties");
                assertTrue(properties.get("CompanyCount") != null);
            }
        }
    }

    @Test(groups = { "functional" })
    public void findWithPopulate() {
        ArrayList<Map<String, String>> paramList = new ArrayList<Map<String, String>>();
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("AttrKey", "Country");
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
        assertTrue(attrList.size() == 2);

        List attrs = (List)attrList.get(0);
        validateAttrs(attrs, "Country", true);
        attrs = (List)attrList.get(1);
        validateAttrs(attrs, "Industry", true);
    }
}
