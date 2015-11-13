package com.latticeengines.admin.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;

public class ServiceResourceTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @Test(groups = "functional")
    public void testUpdateOptions() {
        String url = getRestHostPort() + String.format("/admin/services/%s/options", testLatticeComponent.getName());
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("/Config1");
        field.setOptions(Arrays.asList("1", "2"));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(JsonUtils.serialize(field), headers);

        ResponseEntity<Boolean> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Boolean.class);
        assertTrue(response.getBody());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void testGetServicesWithProducts() {
        Map response = restTemplate.getForObject(getRestHostPort() + "/admin/services/products", Map.class);
        assertNotNull(response);
        System.out.println(response.keySet());
    }

}
