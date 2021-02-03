package com.latticeengines.ulysses.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class LatticeInsightsEnd2EndDeploymentTestNG extends UlyssesDeploymentTestNGBase {
    private List<LeadEnrichmentAttribute> attributes;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void getCategories() throws IOException {
        Set<String> expectedCategoryStrSet = getExpectedCategorySet();

        String url = getUlyssesRestAPIPort() + "/ulysses/latticeinsights/insights/categories";

        List<String> categoryStrList = getOAuth2RestTemplate().getForObject(url, List.class);
        assertNotNull(categoryStrList);

        assertEquals(categoryStrList.size(), expectedCategoryStrSet.size());

        for (String categoryStr : categoryStrList) {
            Assert.assertNotNull(categoryStr);
            Category category = Category.fromName(categoryStr);
            Assert.assertNotNull(category);
            Assert.assertTrue(expectedCategoryStrSet.contains(categoryStr));
            System.out.println("Category with non null attributes : " + categoryStr);
        }
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = "getCategories")
    public void getSubcategories() throws IOException {
        String url = getUlyssesRestAPIPort() + "/ulysses/latticeinsights/insights/subcategories?category="
                + Category.TECHNOLOGY_PROFILE.toString();

        List<String> subcategoryListRaw = getOAuth2RestTemplate().getForObject(url, List.class);
        assertNotNull(subcategoryListRaw);

        List<String> subcategoryStrList = JsonUtils.convertList(subcategoryListRaw, String.class);

        Assert.assertNotNull(subcategoryStrList);

        Assert.assertTrue(subcategoryStrList.size() == 0);
    }

    private Set<String> getExpectedCategorySet() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getAttributes(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        Set<String> expectedCategorySet = new HashSet<>();

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            if (!expectedCategorySet.contains(attr.getCategory())) {
                expectedCategorySet.add(attr.getCategory());
            }
        }

        return expectedCategorySet;
    }

    @Test(groups = "deployment", dependsOnMethods = "getSubcategories")
    public void getAllAttributes() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getAttributes(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
    }

    @Test(groups = "deployment", dependsOnMethods = "getAllAttributes")
    public void customizeAttributes() throws IOException {
        attributes = getAttributes(false);
        String fieldName = attributes.get(0).getFieldName();
        String propertyName1 = "hidden";
        String propertyName2 = "highlighted";
        setProperty(fieldName, propertyName1, "true");
        setProperty(fieldName, propertyName2, "true");
        attributes = getAttributes(false);

        for (LeadEnrichmentAttribute attribute : attributes) {
            if (fieldName.equals(attribute.getFieldName())) {
                JsonNode retrieved = attribute.getAttributeFlagsMap().get(AttributeUseCase.CompanyProfile);
                assertNotNull(retrieved);
                assertEquals(retrieved.get(propertyName1).asBoolean(), true);
                assertEquals(retrieved.get(propertyName2).asBoolean(), true);
            }
        }
    }

    private void setProperty(String fieldName, String propertyName, String propertyValue) {
        String url = getPLSRestAPIPort() + String.format("/pls/attributes/flags/%s/%s/%s", fieldName,
                AttributeUseCase.CompanyProfile, propertyName);
        getGlobalAuthRestTemplate().postForObject(url, propertyValue, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = "customizeAttributes", enabled = false)
    public void retrieveCompanyProfile() {
        CompanyProfileRequest request = new CompanyProfileRequest();
        request.getRecord().put("Email", "someuser@google.com");
        CompanyProfile profile = getOAuth2RestTemplate()
                .postForObject(getUlyssesRestAPIPort() + "/ulysses/companyprofiles/", request, CompanyProfile.class);
        assertNotNull(profile);
        assertNotEquals(profile.getMatchLogs().size(), 0);
        assertNotNull(profile.getTimestamp());

        boolean foundAtLeastOne = false;
        for (LeadEnrichmentAttribute attribute : attributes) {
            boolean found = false;
            for (String fieldName : profile.getAttributes().keySet()) {
                found = found || fieldName.equals(attribute.getFieldName());
            }
            if (found) {
                foundAtLeastOne = true;
            }
        }
        assertTrue(foundAtLeastOne);
    }

    private List<LeadEnrichmentAttribute> getAttributes(boolean onlySelectedAttr) throws IOException {
        return getAttributes(onlySelectedAttr, false);
    }

    private List<LeadEnrichmentAttribute> getAttributes(boolean onlySelectedAttr, boolean considerInternalAttributes)
            throws IOException {
        return getAttributes(onlySelectedAttr, null, null, considerInternalAttributes);
    }

    private List<LeadEnrichmentAttribute> getAttributes(boolean onlySelectedAttr, String attributeDisplayNameFilter,
            Category category, boolean considerInternalAttributes) throws IOException {
        String url = getUlyssesRestAPIPort() + "/ulysses/latticeinsights/insights";
        if (onlySelectedAttr || !StringStandardizationUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)
                || category != null || considerInternalAttributes) {
            url += "?";
        }
        if (onlySelectedAttr) {
            url += "onlySelectedAttributes=" + onlySelectedAttr + "&";
        }
        if (!StringStandardizationUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)) {
            url += "attributeDisplayNameFilter=" + attributeDisplayNameFilter + "&";
        }
        if (category != null) {
            url += "category=" + category.toString() + "&";
        }
        if (considerInternalAttributes) {
            url += "considerInternalAttributes=" + considerInternalAttributes + "&";
        }

        if (url.endsWith("&")) {
            url = url.substring(0, url.length() - 1);
        }

        System.out.println("Using URL: " + url);

        List<?> combinedAttributeObjList = getOAuth2RestTemplate().getForObject(url, List.class);
        assertNotNull(combinedAttributeObjList);

        List<LeadEnrichmentAttribute> combinedAttributeList = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        for (Object obj : combinedAttributeObjList) {
            LeadEnrichmentAttribute attr = om.readValue(om.writeValueAsString(obj), LeadEnrichmentAttribute.class);
            combinedAttributeList.add(attr);
        }

        return combinedAttributeList;
    }
}
