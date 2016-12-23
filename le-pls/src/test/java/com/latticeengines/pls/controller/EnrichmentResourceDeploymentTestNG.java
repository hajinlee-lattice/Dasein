package com.latticeengines.pls.controller;

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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class EnrichmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String SEARCH_DISPLAY_NAME_STR1 = "as AkamaI edge";
    private static final String SEARCH_DISPLAY_NAME_STR2 = " ADP";
    private static final String SEARCH_DISPLAY_NAME_STR3 = "DAY 30 ers ChangE: dAtA";
    private static final String CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3 = "ers 30 DAY ChangE: dAtA";
    private static final String SEARCH_DISPLAY_NAME_STR4 = "as Acc";
    private static final int MAX_DESELECT = 2;
    private static final int MAX_SELECT = 1;
    private static final int MAX_PREMIUM_SELECT = 2;
    private int selectCount = 0;
    private int premiumSelectCount = 0;
    private int deselectCount = 0;
    private int totalLeadEnrichmentCount;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testGetLeadEnrichmentCategories()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        Set<String> expectedCategoryStrSet = getExpectedCategorySet();

        String url = getRestAPIHostPort() + "/pls/enrichment/lead/categories";

        List<String> categoryStrList = restTemplate.getForObject(url, List.class);
        assertNotNull(categoryStrList);

        Assert.assertEquals(categoryStrList.size(), expectedCategoryStrSet.size());

        for (String categoryStr : categoryStrList) {
            Assert.assertNotNull(categoryStr);
            Category category = Category.fromName(categoryStr);
            Assert.assertNotNull(category);
            Assert.assertTrue(expectedCategoryStrSet.contains(categoryStr));
            System.out.println("Category with non null attributes : " + categoryStr);
        }
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testGetLeadEnrichmentSubcategories()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/subcategories?category="
                + Category.TECHNOLOGY_PROFILE.toString();

        List<String> subcategoryListRaw = restTemplate.getForObject(url, List.class);
        assertNotNull(subcategoryListRaw);

        List<String> subcategoryStrList = JsonUtils.convertList(subcategoryListRaw, String.class);

        Assert.assertNotNull(subcategoryStrList);

        Assert.assertTrue(subcategoryStrList.size() > 0);
        System.out.println(subcategoryStrList.get(0));
    }

    private Set<String> getExpectedCategorySet()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
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

    @Test(groups = "deployment", enabled = true)
    public void testGetLeadEnrichmentAttributesBeforeSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertTrue(selectedAttributeList.isEmpty());

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLeadEnrichmentAttributesBeforeSave" })
    public void testGetLeadEnrichmentPremiumAttributesLimitationBeforeSave() {
        checkLimitation();
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentPremiumAttributesLimitationBeforeSave" })
    public void testGetLeadEnrichmentSelectedAttributeCountBeforeSave() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributeCountBeforeSave" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCountBeforeSave() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributePremiumCountBeforeSave" })
    public void testSaveLeadEnrichmentAttributes()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        assertEquals(selectCount, MAX_SELECT);
        assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        assertEquals(deselectCount, 0);
        assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);

        String url = getRestAPIHostPort() + "/pls/enrichment/lead";

        restTemplate.put(url, attributesOperationMap);

        List<LeadEnrichmentAttribute> enrichmentList = getLeadEnrichmentAttributeList(true);
        assertEquals(enrichmentList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);
        enrichmentList = getLeadEnrichmentAttributeList(false);
        assertTrue(enrichmentList.size() > MAX_SELECT + MAX_PREMIUM_SELECT);
        checkSelection(enrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT, MAX_SELECT);

        List<LeadEnrichmentAttribute> selectedEnrichmentList = getLeadEnrichmentAttributeList(true);
        assertEquals(selectedEnrichmentList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);
        checkSelection(selectedEnrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT, MAX_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testSaveLeadEnrichmentAttributes" })
    public void testSaveLeadEnrichmentAttributesFailure()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        String duplicateFieldName = attributesOperationMap.getSelectedAttributes().get(0);
        attributesOperationMap.getDeselectedAttributes().add(duplicateFieldName);

        String url = getRestAPIHostPort() + "/pls/enrichment/lead";

        try {
            restTemplate.put(url, attributesOperationMap);
            assertFalse("Expected exception", true);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(duplicateFieldName));
            assertTrue(ex.getMessage().contains("LEDP_18113"));
        }

        attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        duplicateFieldName = attributesOperationMap.getSelectedAttributes()
                .get(attributesOperationMap.getSelectedAttributes().size() - 1);
        attributesOperationMap.getSelectedAttributes().add(duplicateFieldName);

        url = getRestAPIHostPort() + "/pls/enrichment/lead";

        try {
            restTemplate.put(url, attributesOperationMap);
            assertFalse("Expected exception", true);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(duplicateFieldName));
            assertTrue(ex.getMessage().contains("LEDP_18113"));
        }

        attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        String badFieldName = attributesOperationMap.getSelectedAttributes()
                .get(attributesOperationMap.getSelectedAttributes().size() - 1) + "FAIL";
        attributesOperationMap.getSelectedAttributes().add(badFieldName);

        url = getRestAPIHostPort() + "/pls/enrichment/lead";

        try {
            restTemplate.put(url, attributesOperationMap);
            assertFalse("Expected exception", true);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(badFieldName));
            assertTrue(ex.getMessage().contains("LEDP_18114"));
        }

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testSaveLeadEnrichmentAttributesFailure" })
    public void testGetLeadEnrichmentAttributes()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        assertEquals(combinedAttributeList.size(), totalLeadEnrichmentCount);

        assertEnrichmentList(combinedAttributeList);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertFalse(selectedAttributeList.isEmpty());
        assertEquals(selectedAttributeList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);

        assertEnrichmentList(selectedAttributeList);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLeadEnrichmentAttributes" })
    public void testGetLeadEnrichmentAttributesWithInternalEnrichment()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false, true);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        // TODO - change >= to > once we can create a tenant with feature flag
        assertTrue(combinedAttributeList.size() >= totalLeadEnrichmentCount);

        assertEnrichmentList(combinedAttributeList);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertFalse(selectedAttributeList.isEmpty());
        assertEquals(selectedAttributeList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);

        assertEnrichmentList(selectedAttributeList);
    }

    protected void assertEnrichmentList(List<LeadEnrichmentAttribute> attributeList) {
        for (LeadEnrichmentAttribute attr : attributeList) {
            Assert.assertNotNull(attr.getFieldType());
            Assert.assertNotNull(attr.getFieldJavaType());
            if (!"String".equals(attr.getFieldJavaType())) {
                System.out.println(attr.getFieldJavaType() + " : " + attr.getFieldType());
            }
            Assert.assertNotNull(attr.getCategory());
            Assert.assertNotNull(attr.getSubcategory());
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentAttributesWithInternalEnrichment" })
    public void testGetLeadEnrichmentPremiumAttributesLimitation() {
        checkLimitation();
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentPremiumAttributesLimitation" })
    public void testGetLeadEnrichmentSelectedAttributeCount() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), MAX_SELECT + MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributeCount" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCount() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributePremiumCount" })
    public void testSaveLeadEnrichmentAttributesForSecondSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        assertEquals(selectCount, MAX_SELECT);
        assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        assertEquals(deselectCount, MAX_DESELECT);
        assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);
        assertEquals(attributesOperationMap.getDeselectedAttributes().size(), MAX_DESELECT);

        String url = getRestAPIHostPort() + "/pls/enrichment/lead";

        ObjectMapper om = new ObjectMapper();
        System.out.println("attributesOperationMap = " + om.writeValueAsString(attributesOperationMap));
        restTemplate.put(url, attributesOperationMap);

        List<LeadEnrichmentAttribute> enrichmentList = getLeadEnrichmentAttributeList(true);
        assertEquals(enrichmentList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        enrichmentList = getLeadEnrichmentAttributeList(false);
        assertTrue(enrichmentList.size() > 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        checkSelection(enrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT + 1, MAX_SELECT);

        List<LeadEnrichmentAttribute> selectedEnrichmentList = getLeadEnrichmentAttributeList(true);
        assertEquals(selectedEnrichmentList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        checkSelection(selectedEnrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT + 1, MAX_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testSaveLeadEnrichmentAttributesForSecondSave" })
    public void testGetLeadEnrichmentAttributesAfterSecondSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        assertEquals(combinedAttributeList.size(), totalLeadEnrichmentCount);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertFalse(selectedAttributeList.isEmpty());
        assertEquals(selectedAttributeList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentAttributesAfterSecondSave" })
    public void testGetLeadEnrichmentPremiumAttributesLimitationAfterSecondSave() {
        checkLimitation();
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentPremiumAttributesLimitationAfterSecondSave" })
    public void testGetLeadEnrichmentSelectedAttributeCountAfterSecondSave() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributeCountAfterSecondSave" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCountAfterSecondSave() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 3);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributeCountAfterSecondSave" })
    public void testGetLeadEnrichmentAttributesWithParamsAfterSecondSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false,
                SEARCH_DISPLAY_NAME_STR1, Category.TECHNOLOGY_PROFILE, false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR1 + " - " + attr.getDisplayName());
            assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR1.toUpperCase()));
        }

        assertEquals(combinedAttributeList.size(), 1);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR3,
                Category.INTENT, false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR3 + " - " + attr.getDisplayName());
            assertFalse(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR3.toUpperCase()));
            assertTrue(attr.getDisplayName().toUpperCase().contains(CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3.toUpperCase()));
        }

        assertEquals(combinedAttributeList.size(), 1);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR2,
                Category.TECHNOLOGY_PROFILE, false);
        assertNotNull(combinedAttributeList);
        assertTrue(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR2 + " - " + attr.getDisplayName());
            assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR2.toUpperCase()));
        }

        assertEquals(combinedAttributeList.size(), 0);

        combinedAttributeList = getLeadEnrichmentAttributeList(false, SEARCH_DISPLAY_NAME_STR2,
                Category.TECHNOLOGY_PROFILE, false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR2 + " - " + attr.getDisplayName());
            assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR2.toUpperCase()));
        }

        // this number can change as per update in enrichment metadata table
        assertEquals(combinedAttributeList.size(), 19);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR4,
                Category.TECHNOLOGY_PROFILE, false);
        assertNotNull(combinedAttributeList);
        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR4 + " - " + attr.getDisplayName());
            assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR4.toUpperCase()));
        }
        assertTrue(combinedAttributeList.isEmpty());

        assertEquals(combinedAttributeList.size(), 0);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR1,
                Category.TECHNOLOGY_PROFILE, false);
        assertNotNull(combinedAttributeList);
        assertTrue(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR1 + " - " + attr.getDisplayName());
            assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR1.toUpperCase()));
        }

        assertEquals(combinedAttributeList.size(), 0);

    }

    private LeadEnrichmentAttributesOperationMap pickFewForSelectionFromAllEnrichmentList()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);

        LeadEnrichmentAttributesOperationMap attributesOperationMap = new LeadEnrichmentAttributesOperationMap();
        List<String> newSelectedAttributeList = new ArrayList<>();
        List<String> deselectedAttributeList = new ArrayList<>();
        attributesOperationMap.setSelectedAttributes(newSelectedAttributeList);
        attributesOperationMap.setDeselectedAttributes(deselectedAttributeList);

        selectCount = 0;
        premiumSelectCount = 0;
        deselectCount = 0;

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            if (attr.getIsSelected()) {
                if (deselectCount < MAX_DESELECT) {
                    deselectCount++;
                    attr.setIsSelected(false);
                    deselectedAttributeList.add(attr.getFieldName());
                    System.out.println(
                            "Try to delete" + (attr.getIsPremium() ? " premium" : "") + ": " + attr.getDisplayName());
                }
            } else {
                if (selectCount < MAX_SELECT && !attr.getIsPremium()) {
                    selectCount++;
                    attr.setIsSelected(true);
                    newSelectedAttributeList.add(attr.getFieldName());
                    System.out.println("Try to add: " + attr.getDisplayName());
                } else if (premiumSelectCount < MAX_PREMIUM_SELECT && attr.getIsPremium()) {
                    premiumSelectCount++;
                    attr.setIsSelected(true);
                    attr.setIsPremium(true);
                    newSelectedAttributeList.add(attr.getFieldName());
                    System.out.println("Try to add premium: " + attr.getDisplayName());
                }
            }
        }

        return attributesOperationMap;
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        return getLeadEnrichmentAttributeList(onlySelectedAttr, false);
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr,
            boolean considerInternalAttributes)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        return getLeadEnrichmentAttributeList(onlySelectedAttr, null, null, considerInternalAttributes);
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr,
            String attributeDisplayNameFilter, Category category, boolean considerInternalAttributes)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead";
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

        List<?> combinedAttributeObjList = restTemplate.getForObject(url, List.class);
        assertNotNull(combinedAttributeObjList);

        List<LeadEnrichmentAttribute> combinedAttributeList = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        for (Object obj : combinedAttributeObjList) {
            LeadEnrichmentAttribute attr = om.readValue(om.writeValueAsString(obj), LeadEnrichmentAttribute.class);
            combinedAttributeList.add(attr);
        }

        return combinedAttributeList;
    }

    private void checkSelection(List<LeadEnrichmentAttribute> enrichmentList,
            LeadEnrichmentAttributesOperationMap attributesOperationMap, int premiumSelectCount, int selectCount) {
        for (LeadEnrichmentAttribute attr : enrichmentList) {
            for (String selectedAttr : attributesOperationMap.getSelectedAttributes()) {
                if (attr.getFieldName().equals(selectedAttr)) {
                    assertTrue(attr.getIsSelected());

                    if (attr.getIsPremium()) {
                        premiumSelectCount--;
                        assertTrue(premiumSelectCount >= 0);
                    } else {
                        selectCount--;
                        assertTrue(selectCount >= 0);
                    }
                }
            }

            for (String deselectedAttr : attributesOperationMap.getDeselectedAttributes()) {
                if (attr.getFieldName().equals(deselectedAttr)) {
                    assertFalse(attr.getIsSelected());
                }
            }
        }
    }

    private void checkLimitation() {
        String url = getRestAPIHostPort() + "/pls/enrichment/lead/premiumattributeslimitation";
        @SuppressWarnings("unchecked")
        Map<String, Integer> countMap = restTemplate.getForObject(url, Map.class);
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
