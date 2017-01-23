package com.latticeengines.ulysses.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CompanyProfileAttributeFlags;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class LatticeInsightsResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {
    private int totalLeadEnrichmentCount;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testGetCategories() throws IOException {
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
    @Test(groups = "deployment")
    public void testGetSubcategories() throws IOException {
        String url = getUlyssesRestAPIPort() + "/ulysses/latticeinsights/insights/subcategories?category="
                + Category.TECHNOLOGY_PROFILE.toString();

        List<String> subcategoryListRaw = getOAuth2RestTemplate().getForObject(url, List.class);
        assertNotNull(subcategoryListRaw);

        List<String> subcategoryStrList = JsonUtils.convertList(subcategoryListRaw, String.class);

        Assert.assertNotNull(subcategoryStrList);

        Assert.assertTrue(subcategoryStrList.size() > 0);
        System.out.println(subcategoryStrList.get(0));
    }

    private Set<String> getExpectedCategorySet() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getAttributes(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();
        Set<String> expectedCategorySet = new HashSet<>();

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            if (!expectedCategorySet.contains(attr.getCategory())) {
                expectedCategorySet.add(attr.getCategory());
            }
        }

        return expectedCategorySet;
    }

    @Test(groups = "deployment")
    public void testGetAttributes() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getAttributes(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();

        List<LeadEnrichmentAttribute> selectedAttributeList = getAttributes(true);
        assertNotNull(selectedAttributeList);
        assertTrue(selectedAttributeList.isEmpty());
    }

    @Test(groups = "deployment")
    public void customizeAttributes() throws IOException {
        List<LeadEnrichmentAttribute> attributes = getAttributes(false);
        String fieldName = attributes.get(0).getFieldName();
        CompanyProfileAttributeFlags flags = new CompanyProfileAttributeFlags(true, true);
        setFlags(fieldName, flags);

        attributes = getAttributes(false);

        for (LeadEnrichmentAttribute attribute : attributes) {
            if (fieldName.equals(attribute.getFieldName())) {
                CompanyProfileAttributeFlags retrieved = (CompanyProfileAttributeFlags) attribute.getAttributeFlagsMap()
                        .get(AttributeUseCase.CompanyProfile);
                assertNotNull(retrieved);
                assertEquals(retrieved, flags);
            }
        }
    }

    private void setFlags(String fieldName, CompanyProfileAttributeFlags companyProfileAttributeFlags) {
        String url = getPLSRestAPIPort()
                + String.format("/pls/attributes/flags/%s/%s", fieldName, AttributeUseCase.CompanyProfile);
        getGlobalAuthRestTemplate().postForObject(url, companyProfileAttributeFlags, Void.class);
    }

    @Test(groups = "deployment", dependsOnMethods = {"testGetAttributes"})
    public void testPremiumAttributesLimitation() {
        checkLimitation();
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
        if (onlySelectedAttr || !StringUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter) || category != null
                || considerInternalAttributes) {
            url += "?";
        }
        if (onlySelectedAttr) {
            url += "onlySelectedAttributes=" + onlySelectedAttr + "&";
        }
        if (!StringUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)) {
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

    private void checkLimitation() {
        String url = getUlyssesRestAPIPort() + "/ulysses/latticeinsights/insights/premiumattributeslimitation";
        @SuppressWarnings("unchecked")
        Map<String, Integer> countMap = getOAuth2RestTemplate().getForObject(url, Map.class);
        assertNotNull(countMap);
        assertFalse(countMap.isEmpty());

        boolean foundHGDataSourceInfo = false;

        for (String dataSource : countMap.keySet()) {
            assertFalse(StringUtils.objectIsNullOrEmptyString(dataSource));
            assertNotNull(countMap.get(dataSource));
            assertTrue(countMap.get(dataSource) > 0);

            if (dataSource.equals("HGData_Pivoted_Source")) {
                foundHGDataSourceInfo = true;
            }
        }

        assertTrue(foundHGDataSourceInfo);
    }
}
