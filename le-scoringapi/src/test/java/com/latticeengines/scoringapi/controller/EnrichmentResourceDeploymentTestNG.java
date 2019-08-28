package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class EnrichmentResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final String SEARCH_DISPLAY_NAME_STR1 = "as AkamaI edge";
    private static final String SEARCH_DISPLAY_NAME_STR2 = " ADP";
    private static final String SEARCH_DISPLAY_NAME_STR3 = "Intent Change Users Database";
    private static final String CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3 = "Database Intent Users Change";
    private static final String SEARCH_DISPLAY_NAME_STR4 = "as Acc";
    private static final int MAX_NORMAL_DESELECT = 1;
    private static final int MAX_PREMIUM_DESELECT = 1;
    private static final int MAX_DESELECT = MAX_NORMAL_DESELECT + MAX_PREMIUM_DESELECT;
    private static final int MAX_SELECT = 1;
    private static final int MAX_PREMIUM_SELECT = 2;
    private int selectCount = 0;
    private int premiumSelectCount = 0;
    private int deselectCount = 0;
    private int totalLeadEnrichmentCount;

    @Test(groups = "deployment")
    public void cleanupAttributeSelectionBeforeTest() {
        List<LeadEnrichmentAttribute> existingSelection = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, true, false);
        Assert.assertNotNull(existingSelection);
        Assert.assertEquals(existingSelection.size(), 6);

        LeadEnrichmentAttributesOperationMap deselectedAttributeMap = createDeselectionMap(existingSelection);
        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, deselectedAttributeMap);
        List<LeadEnrichmentAttribute> freshSelection = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, null, true, false);
        Assert.assertNotNull(freshSelection);
        Assert.assertEquals(freshSelection.size(), 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "cleanupAttributeSelectionBeforeTest" })
    public void testGetLeadEnrichmentCategories() throws IOException {
        Set<String> expectedCategoryStrSet = getExpectedCategorySet();

        String url = apiHostPort + "/score/enrichment/categories";
        List<?> categoryListRaw = oAuth2RestTemplate.getForObject(url, List.class);
        List<String> categoryStrList = JsonUtils.convertList(categoryListRaw, String.class);

        Assert.assertNotNull(categoryStrList);

        Assert.assertEquals(categoryStrList.size(), expectedCategoryStrSet.size());

        for (String categoryStr : categoryStrList) {
            Assert.assertNotNull(categoryStr);
            Category category = Category.fromName(categoryStr);
            Assert.assertNotNull(category);
            Assert.assertTrue(expectedCategoryStrSet.contains(categoryStr));
            System.out.println("Category with non null attributes : " + categoryStr);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentCategories" })
    public void testGetLeadEnrichmentSubcategories() {
        String url = apiHostPort + "/score/enrichment/subcategories?category=" + Category.TECHNOLOGY_PROFILE.toString();
        List<?> subcategoryListRaw = oAuth2RestTemplate.getForObject(url, List.class);
        List<String> subcategoryStrList = JsonUtils.convertList(subcategoryListRaw, String.class);

        Assert.assertNotNull(subcategoryStrList);

        Assert.assertTrue(subcategoryStrList.size() > 0);
        System.out.println(subcategoryStrList.get(0));
    }

    private Set<String> getExpectedCategorySet() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();
        Set<String> expectedCategorySet = new HashSet<>();

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            expectedCategorySet.add(attr.getCategory());
        }

        return expectedCategorySet;
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSubcategories" })
    public void testGetLeadEnrichmentAttributesBeforeSave() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        Assert.assertNotNull(selectedAttributeList);
        Assert.assertTrue(selectedAttributeList.isEmpty());

    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentAttributesBeforeSave" })
    public void testGetLeadEnrichmentSelectedAttributeCountBeforeSave() {
        String url = apiHostPort + "/score/enrichment/selectedattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributeCountBeforeSave" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCountBeforeSave() {
        String url = apiHostPort + "/score/enrichment/selectedpremiumattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributePremiumCountBeforeSave" })
    public void testGetLeadEnrichmentPremiumAttributesLimitationBeforeSave() {
        checkLimitation();
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentPremiumAttributesLimitationBeforeSave" })
    public void testSaveLeadEnrichmentAttributes() throws IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        Assert.assertEquals(selectCount, MAX_SELECT);
        Assert.assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        Assert.assertEquals(deselectCount, 0);
        Assert.assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);

        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, attributesOperationMap);

        List<LeadEnrichmentAttribute> enrichmentList = getLeadEnrichmentAttributeList(true);
        Assert.assertEquals(enrichmentList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);
        enrichmentList = getLeadEnrichmentAttributeList(false);
        Assert.assertTrue(enrichmentList.size() > MAX_SELECT + MAX_PREMIUM_SELECT);
        checkSelection(enrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT, MAX_SELECT);

        List<LeadEnrichmentAttribute> selectedEnrichmentList = getLeadEnrichmentAttributeList(true);
        Assert.assertEquals(selectedEnrichmentList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);
        checkSelection(selectedEnrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT, MAX_SELECT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testSaveLeadEnrichmentAttributes" })
    public void testSaveLeadEnrichmentAttributesFailure() throws IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        String duplicateFieldName = attributesOperationMap.getSelectedAttributes().get(0);
        attributesOperationMap.getDeselectedAttributes().add(duplicateFieldName);

        try {
            plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, attributesOperationMap);
            Assert.assertFalse(true, "Expected exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains(duplicateFieldName));
            Assert.assertTrue(ex.getMessage().contains("LEDP_18113"));
        }

        attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        duplicateFieldName = attributesOperationMap.getSelectedAttributes()
                .get(attributesOperationMap.getSelectedAttributes().size() - 1);
        attributesOperationMap.getSelectedAttributes().add(duplicateFieldName);

        try {
            plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, attributesOperationMap);
            Assert.assertFalse(true, "Expected exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains(duplicateFieldName));
            Assert.assertTrue(ex.getMessage().contains("LEDP_18113"));
        }

        attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();
        String badFieldName = attributesOperationMap.getSelectedAttributes()
                .get(attributesOperationMap.getSelectedAttributes().size() - 1) + "FAIL";
        attributesOperationMap.getSelectedAttributes().add(badFieldName);

        try {
            plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, attributesOperationMap);
            Assert.assertFalse(true, "Expected exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains(badFieldName));
            Assert.assertTrue(ex.getMessage().contains("LEDP_18114"));
        }

    }

    @Test(groups = "deployment", dependsOnMethods = { "testSaveLeadEnrichmentAttributesFailure" })
    public void testGetLeadEnrichmentAttributes() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());
        Assert.assertEquals(combinedAttributeList.size(), totalLeadEnrichmentCount);

        assertEnrichmentList(combinedAttributeList);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        Assert.assertNotNull(selectedAttributeList);
        Assert.assertFalse(selectedAttributeList.isEmpty());
        Assert.assertEquals(selectedAttributeList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);

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

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentAttributes" })
    public void testGetLeadEnrichmentPremiumAttributesLimitation() {
        checkLimitation();
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentPremiumAttributesLimitation" })
    public void testGetLeadEnrichmentSelectedAttributeCount() {
        String url = apiHostPort + "/score/enrichment/selectedattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), MAX_SELECT + MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributeCount" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCount() {
        String url = apiHostPort + "/score/enrichment/selectedpremiumattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributePremiumCount" })
    public void testSaveLeadEnrichmentAttributesForSecondSave() throws IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        Assert.assertEquals(selectCount, MAX_SELECT);
        Assert.assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        Assert.assertEquals(deselectCount, MAX_DESELECT);
        Assert.assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);
        Assert.assertEquals(attributesOperationMap.getDeselectedAttributes().size(), MAX_DESELECT);

        ObjectMapper om = new ObjectMapper();
        System.out.println("attributesOperationMap = " + om.writeValueAsString(attributesOperationMap));
        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, attributesOperationMap);

        List<LeadEnrichmentAttribute> enrichmentList = getLeadEnrichmentAttributeList(true);
        Assert.assertEquals(enrichmentList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        enrichmentList = getLeadEnrichmentAttributeList(false);
        Assert.assertTrue(enrichmentList.size() > 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        checkSelection(enrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT + 1, MAX_SELECT);

        List<LeadEnrichmentAttribute> selectedEnrichmentList = getLeadEnrichmentAttributeList(true);
        Assert.assertEquals(selectedEnrichmentList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        checkSelection(selectedEnrichmentList, attributesOperationMap, MAX_PREMIUM_SELECT + 1, MAX_SELECT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testSaveLeadEnrichmentAttributesForSecondSave" })
    public void testGetLeadEnrichmentAttributesAfterSecondSave() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());
        Assert.assertEquals(combinedAttributeList.size(), totalLeadEnrichmentCount);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        Assert.assertNotNull(selectedAttributeList);
        Assert.assertFalse(selectedAttributeList.isEmpty());
        Assert.assertEquals(selectedAttributeList.size(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
        for (LeadEnrichmentAttribute attr : selectedAttributeList) {
            Assert.assertNotNull(attr.getCategory());
            Assert.assertNotNull(attr.getSubcategory(), attr.getFieldName() + " should not have empty subcategory");
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentAttributesAfterSecondSave" })
    public void testGetLeadEnrichmentPremiumAttributesLimitationAfterSecondSave() {
        checkLimitation();
    }

    @Test(groups = "deployment", dependsOnMethods = {
            "testGetLeadEnrichmentPremiumAttributesLimitationAfterSecondSave" })
    public void testGetLeadEnrichmentSelectedAttributeCountAfterSecondSave() {
        String url = apiHostPort + "/score/enrichment/selectedattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLeadEnrichmentSelectedAttributeCountAfterSecondSave" })
    public void testGetLeadEnrichmentSelectedAttributePremiumCountAfterSecondSave() {
        String url = apiHostPort + "/score/enrichment/selectedpremiumattributes/count";
        Integer count = oAuth2RestTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(count);
        Assert.assertEquals(count.intValue(), 3);
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = {
            "testGetLeadEnrichmentSelectedAttributePremiumCountAfterSecondSave" })
    public void testGetLeadEnrichmentAttributesWithParamsAfterSecondSave() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false,
                SEARCH_DISPLAY_NAME_STR1, Category.TECHNOLOGY_PROFILE);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR1 + " - " + attr.getDisplayName());
            Assert.assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR1.toUpperCase()));
        }

        Assert.assertEquals(combinedAttributeList.size(), 1);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3,
                Category.INTENT);
        Assert.assertNotNull(combinedAttributeList);
        System.out.println(
                "Check for " + CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3 + " size is " + combinedAttributeList.size());
        Assert.assertTrue(combinedAttributeList.size() > 0);
        Assert.assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR3 + " - " + attr.getDisplayName());
            Assert.assertFalse(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR3.toUpperCase()));
            Assert.assertTrue(
                    attr.getDisplayName().toUpperCase().contains(CORRECT_ORDER_SEARCH_DISPLAY_NAME_STR3.toUpperCase()));
        }

        Assert.assertEquals(combinedAttributeList.size(), 1);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR2,
                Category.TECHNOLOGY_PROFILE);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertTrue(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR2 + " - " + attr.getDisplayName());
            Assert.assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR2.toUpperCase()));
        }

        Assert.assertEquals(combinedAttributeList.size(), 0);

        combinedAttributeList = getLeadEnrichmentAttributeList(false, SEARCH_DISPLAY_NAME_STR2,
                Category.TECHNOLOGY_PROFILE);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR2 + " - " + attr.getDisplayName());
            Assert.assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR2.toUpperCase()));
        }

        // this number can change as per update in enrichment metadata table
        Assert.assertEquals(combinedAttributeList.size(), 19);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR4,
                Category.TECHNOLOGY_PROFILE);
        Assert.assertNotNull(combinedAttributeList);
        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR4 + " - " + attr.getDisplayName());
            Assert.assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR4.toUpperCase()));
        }
        Assert.assertTrue(combinedAttributeList.isEmpty());

        Assert.assertEquals(combinedAttributeList.size(), 0);

        combinedAttributeList = getLeadEnrichmentAttributeList(true, SEARCH_DISPLAY_NAME_STR1,
                Category.TECHNOLOGY_PROFILE);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertTrue(combinedAttributeList.isEmpty());

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            System.out.println("Check for " + SEARCH_DISPLAY_NAME_STR1 + " - " + attr.getDisplayName());
            Assert.assertTrue(attr.getDisplayName().toUpperCase().contains(SEARCH_DISPLAY_NAME_STR1.toUpperCase()));
        }

        Assert.assertEquals(combinedAttributeList.size(), 0);

    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = {
            "testGetLeadEnrichmentAttributesWithParamsAfterSecondSave" })
    public void testPagination() throws IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        Assert.assertNotNull(combinedAttributeList);
        Assert.assertFalse(combinedAttributeList.isEmpty());

        int fullSize = getLeadEnrichmentAttributeListCount(false, null, null);

        Assert.assertEquals(fullSize, combinedAttributeList.size());

        List<LeadEnrichmentAttribute> fullPageAttributeList = getLeadEnrichmentAttributeList(false, null, null, null, 0,
                fullSize);
        Assert.assertNotNull(fullPageAttributeList);
        Assert.assertFalse(fullPageAttributeList.isEmpty());
        Assert.assertEquals(fullPageAttributeList.size(), fullSize);

        for (int i = 0; i < fullSize; i++) {
            LeadEnrichmentAttribute withoutPageAttr = combinedAttributeList.get(i);
            LeadEnrichmentAttribute withPageAttr = fullPageAttributeList.get(i);

            Assert.assertEquals(withoutPageAttr.getFieldName(), withPageAttr.getFieldName());
            Assert.assertEquals(withoutPageAttr.getDisplayName(), withPageAttr.getDisplayName());
            Assert.assertEquals(withoutPageAttr.getCategory(), withPageAttr.getCategory());
            Assert.assertEquals(withoutPageAttr.getIsSelected(), withPageAttr.getIsSelected());
        }

        int offset = 5;
        int max = fullSize - offset - 10;

        List<LeadEnrichmentAttribute> partialPageAttributeList = getLeadEnrichmentAttributeList(false, null, null, null,
                offset, max);
        Assert.assertNotNull(partialPageAttributeList);
        Assert.assertFalse(partialPageAttributeList.isEmpty());
        Assert.assertEquals(partialPageAttributeList.size(), max);

        for (int i = offset; i < offset + max; i++) {
            LeadEnrichmentAttribute withoutPageAttr = combinedAttributeList.get(i);
            LeadEnrichmentAttribute withPageAttr = partialPageAttributeList.get(i - offset);

            Assert.assertEquals(withoutPageAttr.getFieldName(), withPageAttr.getFieldName());
            Assert.assertEquals(withoutPageAttr.getDisplayName(), withPageAttr.getDisplayName());
            Assert.assertEquals(withoutPageAttr.getCategory(), withPageAttr.getCategory());
            Assert.assertEquals(withoutPageAttr.getIsSelected(), withPageAttr.getIsSelected());
        }

        List<LeadEnrichmentAttribute> combinedAttributeListSelected = getLeadEnrichmentAttributeList(true);
        Assert.assertNotNull(combinedAttributeListSelected);
        Assert.assertFalse(combinedAttributeListSelected.isEmpty());

        int fullSizeSelected = getLeadEnrichmentAttributeListCount(true, null, null);

        Assert.assertEquals(fullSizeSelected, combinedAttributeListSelected.size());

        List<LeadEnrichmentAttribute> fullPageAttributeListSelected = getLeadEnrichmentAttributeList(true, null, null,
                "DUMMY_STR_FOR_NOW", 0, fullSizeSelected);
        Assert.assertNotNull(fullPageAttributeListSelected);
        Assert.assertFalse(fullPageAttributeListSelected.isEmpty());
        Assert.assertEquals(fullPageAttributeListSelected.size(), fullSizeSelected);

        for (int i = 0; i < fullSizeSelected; i++) {
            LeadEnrichmentAttribute withoutPageAttr = combinedAttributeListSelected.get(i);
            LeadEnrichmentAttribute withPageAttr = fullPageAttributeListSelected.get(i);

            Assert.assertEquals(withoutPageAttr.getFieldName(), withPageAttr.getFieldName());
            Assert.assertEquals(withoutPageAttr.getDisplayName(), withPageAttr.getDisplayName());
            Assert.assertEquals(withoutPageAttr.getCategory(), withPageAttr.getCategory());
            Assert.assertEquals(withoutPageAttr.getIsSelected(), withPageAttr.getIsSelected());
        }

        int offsetSelected = 1;
        int maxSelected = fullSizeSelected - offsetSelected - 1;

        List<LeadEnrichmentAttribute> partialPageAttributeListSelected = getLeadEnrichmentAttributeList(true, null,
                null, "DUMMY_STR_FOR_NOW", offsetSelected, maxSelected);
        Assert.assertNotNull(partialPageAttributeListSelected);
        Assert.assertFalse(partialPageAttributeListSelected.isEmpty());
        Assert.assertEquals(partialPageAttributeListSelected.size(), maxSelected);

        for (int i = offsetSelected; i < offsetSelected + maxSelected; i++) {
            LeadEnrichmentAttribute withoutPageAttr = combinedAttributeListSelected.get(i);
            LeadEnrichmentAttribute withPageAttr = partialPageAttributeListSelected.get(i - offsetSelected);

            Assert.assertEquals(withoutPageAttr.getFieldName(), withPageAttr.getFieldName());
            Assert.assertEquals(withoutPageAttr.getDisplayName(), withPageAttr.getDisplayName());
            Assert.assertEquals(withoutPageAttr.getCategory(), withPageAttr.getCategory());
            Assert.assertEquals(withoutPageAttr.getIsSelected(), withPageAttr.getIsSelected());
        }
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
        int deselectNormalCount = 0;
        int deselectPremiumCount = 0;

        for (LeadEnrichmentAttribute attr : combinedAttributeList) {
            if (attr.getIsSelected()) {
                if (deselectNormalCount < MAX_NORMAL_DESELECT && !attr.getIsPremium()) {
                    deselectNormalCount++;
                    attr.setIsSelected(false);
                    deselectedAttributeList.add(attr.getFieldName());
                    System.out.println(
                            String.format("Try to delete: %s (%s)", attr.getFieldName(), attr.getDisplayName()));
                } else if (deselectPremiumCount < MAX_PREMIUM_DESELECT && attr.getIsPremium()) {
                    deselectPremiumCount++;
                    attr.setIsSelected(false);
                    deselectedAttributeList.add(attr.getFieldName());
                    System.out.println(String.format("Try to delete premium: %s (%s)", attr.getFieldName(),
                            attr.getDisplayName()));
                }
            } else {
                if (selectCount < MAX_SELECT && !attr.getIsPremium()) {
                    selectCount++;
                    attr.setIsSelected(true);
                    newSelectedAttributeList.add(attr.getFieldName());
                    System.out
                            .println(String.format("Try to add: %s (%s)", attr.getFieldName(), attr.getDisplayName()));
                } else if (premiumSelectCount < MAX_PREMIUM_SELECT && attr.getIsPremium()) {
                    premiumSelectCount++;
                    attr.setIsSelected(true);
                    attr.setIsPremium(true);
                    newSelectedAttributeList.add(attr.getFieldName());
                    System.out.println(
                            String.format("Try to add premium: %s (%s)", attr.getFieldName(), attr.getDisplayName()));
                }
            }
        }

        deselectCount = deselectNormalCount + deselectPremiumCount;

        return attributesOperationMap;
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        return getLeadEnrichmentAttributeList(onlySelectedAttr, null, null);
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr,
            String attributeDisplayNameFilter, Category category)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        return getLeadEnrichmentAttributeList(onlySelectedAttr, attributeDisplayNameFilter, category, null, null, null);
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr,
            String attributeDisplayNameFilter, Category category, String subcategory, Integer offset, Integer max)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = apiHostPort + "/score/enrichment";
        if (onlySelectedAttr || !StringStandardizationUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)
                || category != null || offset != null || max != null) {
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
            if (!StringStandardizationUtils.objectIsNullOrEmptyString(subcategory)) {
                url += "subcategory=" + subcategory + "&";
            }
        }
        if (offset != null) {
            url += "offset=" + offset.intValue() + "&";
        }
        if (max != null) {
            url += "max=" + max.intValue() + "&";
        }

        if (url.endsWith("&")) {
            url = url.substring(0, url.length() - 1);
        }

        System.out.println("Using URL: " + url);

        List<?> combinedAttributeObjList = oAuth2RestTemplate.getForObject(url, List.class);
        Assert.assertNotNull(combinedAttributeObjList);

        List<LeadEnrichmentAttribute> combinedAttributeList = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        for (Object obj : combinedAttributeObjList) {
            LeadEnrichmentAttribute attr = om.readValue(om.writeValueAsString(obj), LeadEnrichmentAttribute.class);
            combinedAttributeList.add(attr);
        }

        return combinedAttributeList;
    }

    private int getLeadEnrichmentAttributeListCount(boolean onlySelectedAttr, String attributeDisplayNameFilter,
            Category category) {
        String url = apiHostPort + "/score/enrichment/count";
        if (onlySelectedAttr || !StringStandardizationUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)
                || category != null) {
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

        if (url.endsWith("&")) {
            url = url.substring(0, url.length() - 1);
        }

        System.out.println("Using URL: " + url);

        return oAuth2RestTemplate.getForObject(url, Integer.class);
    }

    private void checkSelection(List<LeadEnrichmentAttribute> enrichmentList,
            LeadEnrichmentAttributesOperationMap attributesOperationMap, int premiumSelectCount, int selectCount) {
        for (LeadEnrichmentAttribute attr : enrichmentList) {
            for (String selectedAttr : attributesOperationMap.getSelectedAttributes()) {
                if (attr.getFieldName().equals(selectedAttr)) {
                    Assert.assertTrue(attr.getIsSelected());

                    if (attr.getIsPremium()) {
                        premiumSelectCount--;
                        Assert.assertTrue(premiumSelectCount >= 0);
                    } else {
                        selectCount--;
                        Assert.assertTrue(selectCount >= 0);
                    }
                }
            }

            for (String deselectedAttr : attributesOperationMap.getDeselectedAttributes()) {
                if (attr.getFieldName().equals(deselectedAttr)) {
                    Assert.assertFalse(attr.getIsSelected());
                }
            }
        }
    }

    private void checkLimitation() {
        String url = apiHostPort + "/score/enrichment/premiumattributeslimitation";
        @SuppressWarnings("unchecked")
        Map<String, Integer> countMap = oAuth2RestTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(countMap);
        Assert.assertFalse(countMap.isEmpty());

        boolean foundHGDataSourceInfo = false;

        for (String dataSource : countMap.keySet()) {
            Assert.assertFalse(StringStandardizationUtils.objectIsNullOrEmptyString(dataSource));
            Assert.assertNotNull(countMap.get(dataSource));
            Assert.assertTrue(countMap.get(dataSource) > 0);

            if (dataSource.equals("HGData_Pivoted_Source")) {
                foundHGDataSourceInfo = true;
            }
        }

        Assert.assertTrue(foundHGDataSourceInfo);
    }

    private LeadEnrichmentAttributesOperationMap createDeselectionMap(List<LeadEnrichmentAttribute> existingSelection) {
        LeadEnrichmentAttributesOperationMap deselectedAttributeMap = new LeadEnrichmentAttributesOperationMap();
        List<String> selectedAttrs = new ArrayList<>();
        List<String> deselectedAttrs = new ArrayList<>();
        deselectedAttributeMap.setSelectedAttributes(selectedAttrs);
        deselectedAttributeMap.setDeselectedAttributes(deselectedAttrs);
        for (LeadEnrichmentAttribute attr : existingSelection) {
            deselectedAttrs.add(attr.getFieldName());
        }
        return deselectedAttributeMap;
    }
}
