package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class LeadEnrichmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final int MAX_DESELECT = 5;
    private static final int MAX_SELECT = 4;
    private static final int MAX_PREMIUM_SELECT = 2;
    private int selectCount = 0;
    private int premiumSelectCount = 0;
    private int deselectCount = 0;
    private int totalLeadEnrichmentCount;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA);
    }

    // ------------START for LP v2-------------------//
    @Test(groups = "deployment", enabled = false)
    public void testGetAvariableAttributes() {
        switchToSuperAdmin();
        assertGetAvariableAttributesSuccess();

        switchToInternalAdmin();
        assertGetAvariableAttributesSuccess();

        switchToInternalUser();
        assertGetAvariableAttributesGet403();

        switchToExternalAdmin();
        assertGetAvariableAttributesSuccess();

        switchToExternalUser();
        assertGetAvariableAttributesGet403();
    }

    @SuppressWarnings("unchecked")
    private void assertGetAvariableAttributesSuccess() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/avariableattributes";
        List<LeadEnrichmentAttribute> attributes = restTemplate.getForObject(url, List.class);
        assertNotNull(attributes);
    }

    private void assertGetAvariableAttributesGet403() {
        boolean exception = false;
        try {
            String url = getRestAPIHostPort() + "/pls/leadenrichment/avariableattributes";
            restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @Test(groups = "deployment", enabled = false)
    public void testGetAttributes() {
        switchToSuperAdmin();
        assertGetAttributesSuccess();

        switchToInternalAdmin();
        assertGetAttributesSuccess();

        switchToInternalUser();
        assertGetAttributesGet403();

        switchToExternalAdmin();
        assertGetAttributesSuccess();

        switchToExternalUser();
        assertGetAttributesGet403();
    }

    @SuppressWarnings("unchecked")
    private void assertGetAttributesSuccess() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/attributes";
        List<LeadEnrichmentAttribute> attributes = restTemplate.getForObject(url, List.class);
        assertNotNull(attributes);
    }

    private void assertGetAttributesGet403() {
        boolean exception = false;
        try {
            String url = getRestAPIHostPort() + "/pls/leadenrichment/attributes";
            restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = false)
    public void testVerifyAttributes() {
        // Target tables:
        // Marketo --> LeadRecord
        // Eloqua --> Contact
        // SFDC --> Contact, Lead
        // The main test tenant is Marketo.
        switchToSuperAdmin();
        LeadEnrichmentAttribute[] attributes = new LeadEnrichmentAttribute[3];
        LeadEnrichmentAttribute attribute = new LeadEnrichmentAttribute();
        attribute.setFieldName("Country");
        attributes[0] = attribute;
        attribute = new LeadEnrichmentAttribute();
        attribute.setFieldName("city");
        attributes[1] = attribute;
        attribute = new LeadEnrichmentAttribute();
        String noExistColumn = "ColumnDoesNotExist_!@#";
        attribute.setFieldName(noExistColumn);
        attributes[2] = attribute;
        String url = getRestAPIHostPort() + "/pls/leadenrichment/verifyattributes";
        Map<String, List<String>> map = restTemplate.postForObject(url, attributes, Map.class);
        assertNotNull(map);
        for (Entry<String, List<String>> entry : map.entrySet()) {
            List<String> invalidFields = entry.getValue();
            assertNotNull(invalidFields);
            assertEquals(invalidFields.size(), 1);
            assertEquals(invalidFields.get(0), noExistColumn);
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void testSaveAttributes() {
        switchToSuperAdmin();
        String avariableUrl = getRestAPIHostPort() + "/pls/leadenrichment/avariableattributes";
        LeadEnrichmentAttribute[] avariableAttributes = restTemplate.getForObject(avariableUrl,
                LeadEnrichmentAttribute[].class);
        if (avariableAttributes != null && avariableAttributes.length > 0) {
            Map<String, LeadEnrichmentAttribute> avariableAttrsMap = new HashMap<String, LeadEnrichmentAttribute>();
            for (LeadEnrichmentAttribute attribute : avariableAttributes) {
                avariableAttrsMap.put(attribute.getFieldName(), attribute);
            }
            String url = getRestAPIHostPort() + "/pls/leadenrichment/attributes";
            LeadEnrichmentAttribute[] attributes = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            for (LeadEnrichmentAttribute attribute : attributes) {
                String key = attribute.getFieldName();
                if (avariableAttrsMap.containsKey(key)) {
                    attribute.setDataSource(avariableAttrsMap.get(key).getDataSource());
                } else {
                    return;
                }
            }

            assertSaveAttributesSuccess(url, attributes, avariableAttributes);

            switchToInternalUser();
            assertSaveAttributesGet403(url, attributes);

            switchToExternalUser();
            assertSaveAttributesGet403(url, attributes);
        }
    }

    private void assertSaveAttributesSuccess(String url, LeadEnrichmentAttribute[] attributes,
            LeadEnrichmentAttribute[] avariableAttributes) {
        // Clear attributes
        List<LeadEnrichmentAttribute> attrsToSave = new ArrayList<LeadEnrichmentAttribute>();
        restTemplate.put(url, attrsToSave, new HashMap<>());
        LeadEnrichmentAttribute[] attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
        assertEquals(attrsToSave.size(), attrsSaved.length);

        // Get 2 attributes with 2 different attributes.
        for (LeadEnrichmentAttribute attr : avariableAttributes) {
            if (attrsToSave.size() == 0) {
                attrsToSave.add(attr);
            } else if (!attr.getDataSource().equals(attrsToSave.get(0).getDataSource())) {
                attrsToSave.add(attr);
                break;
            }
        }
        if (attrsToSave.size() == 2) {
            restTemplate.put(url, attrsToSave, new HashMap<>());
            attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            assertEquals(attrsToSave.size(), attrsSaved.length);
            // Test we cannot remove one of 2 attributes with 2 different data
            // source
            attrsToSave.remove(0);
            restTemplate.put(url, attrsToSave, new HashMap<>());
            attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            assertEquals(attrsToSave.size(), attrsSaved.length);
        }

        // Roll back
        restTemplate.put(url, attributes, new HashMap<>());
    }

    private void assertSaveAttributesGet403(String url, LeadEnrichmentAttribute[] attributes) {
        boolean exception = false;
        try {
            restTemplate.put(url, attributes, new HashMap<>());
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @Test(groups = "deployment")
    public void testGetPremiumAttributesLimitation() {
        switchToSuperAdmin();
        assertGetPremiumAttributesLimitationSuccess();
    }

    private void assertGetPremiumAttributesLimitationSuccess() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/premiumattributeslimitation";
        Integer limitation = restTemplate.getForObject(url, Integer.class);
        assertNotNull(limitation);
    }

    // ------------END for LP v2-------------------//

    // ------------START for LP v3-------------------//
    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testGetLP3Categories() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/categories";
        List<String> categoryStrList = restTemplate.getForObject(url, List.class);
        assertNotNull(categoryStrList);

        Assert.assertEquals(categoryStrList.size(), Category.values().length);

        for (String categoryStr : categoryStrList) {
            Assert.assertNotNull(categoryStr);
            Category category = Category.fromName(categoryStr);
            Assert.assertNotNull(category);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetLP3AttributesBeforeSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        totalLeadEnrichmentCount = combinedAttributeList.size();

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertTrue(selectedAttributeList.isEmpty());

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3AttributesBeforeSave" })
    public void testGetLP3PremiumAttributesLimitationBeforeSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/premiumattributeslimitation";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLP3PremiumAttributesLimitationBeforeSave" })
    public void testGetLP3SelectedAttributeCountBeforeSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3SelectedAttributeCountBeforeSave" })
    public void testGetLP3SelectedAttributePremiumCountBeforeSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 0);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLP3SelectedAttributePremiumCountBeforeSave" })
    public void testSaveLP3Attributes()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        assertEquals(selectCount, MAX_SELECT);
        assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        assertEquals(deselectCount, 0);
        assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);

        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3";

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

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testSaveLP3Attributes" })
    public void testGetLP3Attributes()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);
        assertNotNull(combinedAttributeList);
        assertFalse(combinedAttributeList.isEmpty());
        assertEquals(combinedAttributeList.size(), totalLeadEnrichmentCount);

        List<LeadEnrichmentAttribute> selectedAttributeList = getLeadEnrichmentAttributeList(true);
        assertNotNull(selectedAttributeList);
        assertFalse(selectedAttributeList.isEmpty());
        assertEquals(selectedAttributeList.size(), MAX_SELECT + MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3Attributes" })
    public void testGetLP3PremiumAttributesLimitation() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/premiumattributeslimitation";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3PremiumAttributesLimitation" })
    public void testGetLP3SelectedAttributeCount() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), MAX_SELECT + MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3SelectedAttributeCount" })
    public void testGetLP3SelectedAttributePremiumCount() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), MAX_PREMIUM_SELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3SelectedAttributePremiumCount" })
    public void testSaveLP3AttributesForSecondSave()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        LeadEnrichmentAttributesOperationMap attributesOperationMap = pickFewForSelectionFromAllEnrichmentList();

        assertEquals(selectCount, MAX_SELECT);
        assertEquals(premiumSelectCount, MAX_PREMIUM_SELECT);
        assertEquals(deselectCount, MAX_DESELECT);
        assertEquals(attributesOperationMap.getSelectedAttributes().size(), MAX_PREMIUM_SELECT + MAX_SELECT);
        assertEquals(attributesOperationMap.getDeselectedAttributes().size(), MAX_DESELECT);

        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3";

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

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testSaveLP3AttributesForSecondSave" })
    public void testGetLP3AttributesAfterSecondSave()
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

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetLP3AttributesAfterSecondSave" })
    public void testGetLP3PremiumAttributesLimitationAfterSecondSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/premiumattributeslimitation";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLP3PremiumAttributesLimitationAfterSecondSave" })
    public void testGetLP3SelectedAttributeCountAfterSecondSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), 2 * (MAX_SELECT + MAX_PREMIUM_SELECT) - MAX_DESELECT);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testGetLP3SelectedAttributeCountAfterSecondSave" })
    public void testGetLP3SelectedAttributePremiumCountAfterSecondSave() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedpremiumattributes/count";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
        assertEquals(count.intValue(), MAX_PREMIUM_SELECT + (MAX_SELECT + MAX_PREMIUM_SELECT - MAX_DESELECT));
    }

    private LeadEnrichmentAttributesOperationMap pickFewForSelectionFromAllEnrichmentList()
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {

        List<LeadEnrichmentAttribute> combinedAttributeList = getLeadEnrichmentAttributeList(false);

        LeadEnrichmentAttributesOperationMap attributesOperationMap = new LeadEnrichmentAttributesOperationMap();
        List<LeadEnrichmentAttribute> newSelectedAttributeList = new ArrayList<>();
        List<LeadEnrichmentAttribute> deselectedAttributeList = new ArrayList<>();
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
                    deselectedAttributeList.add(attr);
                }
            } else {
                if (selectCount < MAX_SELECT && !attr.getIsPremium()) {
                    selectCount++;
                    attr.setIsSelected(true);
                    newSelectedAttributeList.add(attr);
                } else if (premiumSelectCount < MAX_PREMIUM_SELECT && attr.getIsPremium()) {
                    premiumSelectCount++;
                    attr.setIsSelected(true);
                    attr.setIsPremium(true);
                    newSelectedAttributeList.add(attr);
                }
            }
        }

        return attributesOperationMap;
    }

    private List<LeadEnrichmentAttribute> getLeadEnrichmentAttributeList(boolean onlySelectedAttr)
            throws JsonParseException, JsonMappingException, JsonProcessingException, IOException {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3";
        if (onlySelectedAttr) {
            url += "?onlySelectedAttributes=" + onlySelectedAttr;
        }
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
            for (LeadEnrichmentAttribute selectedAttr : attributesOperationMap.getSelectedAttributes()) {
                if (attr.getFieldName().equals(selectedAttr.getFieldName())) {
                    assertTrue(attr != selectedAttr);
                    assertEquals(attr.getIsSelected(), selectedAttr.getIsSelected());
                    assertEquals(attr.getIsPremium(), selectedAttr.getIsPremium());
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

            for (LeadEnrichmentAttribute deselectedAttr : attributesOperationMap.getDeselectedAttributes()) {
                if (attr.getFieldName().equals(deselectedAttr.getFieldName())) {
                    assertTrue(attr != deselectedAttr);
                    assertEquals(attr.getIsSelected(), deselectedAttr.getIsSelected());
                    assertEquals(attr.getIsPremium(), deselectedAttr.getIsPremium());
                    assertFalse(attr.getIsSelected());
                }
            }
        }
    }

    // ------------END for LP v3-------------------//
}
