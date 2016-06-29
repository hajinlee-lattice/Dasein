package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributeOperation;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class LeadEnrichmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

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
        List<String> categoryList = restTemplate.getForObject(url, List.class);
        assertNotNull(categoryList);
    }

    @Test(groups = "deployment", enabled = true)
    public void testSaveLP3Attributes() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3";
        Map<LeadEnrichmentAttributeOperation, List<LeadEnrichmentAttribute>> attributesOperationMap = new HashMap<>();
        List<LeadEnrichmentAttribute> newSelectedAttributeList = new ArrayList<>();
        LeadEnrichmentAttribute attr1 = new LeadEnrichmentAttribute();
        LeadEnrichmentAttribute attr2 = new LeadEnrichmentAttribute();
        newSelectedAttributeList.add(attr1);
        newSelectedAttributeList.add(attr2);

        List<LeadEnrichmentAttribute> unselectedAttributeList = new ArrayList<>();
        LeadEnrichmentAttribute attr3 = new LeadEnrichmentAttribute();
        LeadEnrichmentAttribute attr4 = new LeadEnrichmentAttribute();
        unselectedAttributeList.add(attr3);
        unselectedAttributeList.add(attr4);

        attributesOperationMap.put(LeadEnrichmentAttributeOperation.SELECT, newSelectedAttributeList);
        attributesOperationMap.put(LeadEnrichmentAttributeOperation.DESELECT, unselectedAttributeList);

        restTemplate.put(url, attributesOperationMap);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", enabled = true)
    public void testGetLP3Attributes() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3";
        List<LeadEnrichmentAttribute> combinedAttributeList = restTemplate.getForObject(url, List.class);
        assertNotNull(combinedAttributeList);
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetLP3PremiumAttributesLimitation() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/premiumattributeslimitation";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetLP3SelectedAttributeCount() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedAttributeCount";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }

    @Test(groups = "deployment", enabled = true)
    public void testGetLP3SelectedAttributePremiumCount() {
        String url = getRestAPIHostPort() + "/pls/leadenrichment/v3/selectedPremiumAttributeCount";
        Integer count = restTemplate.getForObject(url, Integer.class);
        assertNotNull(count);
    }
    // ------------END for LP v3-------------------//
}
