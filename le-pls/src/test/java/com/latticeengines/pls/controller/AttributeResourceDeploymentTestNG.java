package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class AttributeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private String attributeName;
    private String categoryName;

    private String propertyName = "hidden";
    private String propertyValue = Boolean.TRUE.toString();

    private Tenant secondTenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);

        System.out.println("Retrieving attributes...");
        List<?> raw = testBed.getRestTemplate()
                .getForObject(getRestAPIHostPort() + "/pls/latticeinsights/insights", List.class);
        List<LeadEnrichmentAttribute> attributes = JsonUtils.convertList(raw,
                LeadEnrichmentAttribute.class);
        attributeName = attributes.get(0).getFieldName();
        categoryName = attributes.get(0).getCategory();
    }

    @Test(groups = "deployment")
    public void testSave() {
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, propertyName);
        testBed.getRestTemplate().postForObject(url, propertyValue, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "testSave")
    public void testRetrieve() {
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, propertyName);
        String retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"), propertyValue);
    }

    @Test(groups = "deployment", dependsOnMethods = "testRetrieve")
    public void testSaveMultiple() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hidden", Boolean.TRUE.toString());
        properties.put("highlighted", Boolean.TRUE.toString());

        String url = getRestAPIHostPort()
                + String.format("/pls/attributes/flags/%s/CompanyProfile", attributeName);
        testBed.getRestTemplate().postForObject(url, properties, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "testSaveMultiple")
    public void testRetrieveAfterSaveMultiple() {
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, "hidden");
        String retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"),
                Boolean.TRUE.toString());

        url = getRestAPIHostPort() + String.format("/pls/attributes/flags/%s/CompanyProfile/%s",
                attributeName, "highlighted");
        retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"),
                Boolean.TRUE.toString());
    }

    @Test(groups = "deployment", dependsOnMethods = "testRetrieveAfterSaveMultiple")
    public void testSaveCategoryMultiple() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hidden", Boolean.TRUE.toString());
        properties.put("highlighted", Boolean.FALSE.toString());

        String url = getRestAPIHostPort() + String.format(
                "/pls/attributes/categories/flags/CompanyProfile?category=%s", categoryName);
        testBed.getRestTemplate().postForObject(url, properties, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "testSaveCategoryMultiple")
    public void testRetrieveAfterSaveCategoryMultiple() {
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, "hidden");
        String retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"),
                Boolean.TRUE.toString());

        url = getRestAPIHostPort() + String.format("/pls/attributes/flags/%s/CompanyProfile/%s",
                attributeName, "highlighted");
        retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"),
                Boolean.FALSE.toString());
    }

    @Test(groups = "deployment", dependsOnMethods = "testRetrieveAfterSaveCategoryMultiple")
    public void testOtherTenantUnaffected() {
        testBed.bootstrapForProduct(LatticeProduct.LPA3);
        secondTenant = testBed.getTestTenants().get(1);

        testBed.switchToExternalAdmin(secondTenant);
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, "hidden");
        String retrieved = testBed.getRestTemplate().getForObject(url, String.class);
        assertNotNull(retrieved);
        assertEquals(JsonUtils.deserialize(retrieved, HashMap.class).get("value"),
                Boolean.FALSE.toString());
    }

    @Test(groups = "deployment", expectedExceptions = Exception.class)
    public void testSaveInvalidProperty() {
        String url = getRestAPIHostPort() + String
                .format("/pls/attributes/flags/%s/CompanyProfile/%s", attributeName, "poop");
        testBed.getRestTemplate().postForObject(url, propertyValue, Void.class);
    }
}
