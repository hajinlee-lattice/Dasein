package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class LeadEnrichmentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        switchToSuperAdmin();
    }

    @Test(groups = "deployment")
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

    @Test(groups = "deployment", enabled = false)
    public void testSaveAttributes() {
        switchToSuperAdmin();
        String url = getRestAPIHostPort() + "/pls/leadenrichment/attributes";
        LeadEnrichmentAttribute[] attributes = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
        if (attributes != null && attributes.length > 0) {
            assertSaveAttributesSuccess(url, attributes);

            switchToInternalUser();
            assertSaveAttributesGet403(url, attributes);

            switchToExternalUser();
            assertSaveAttributesGet403(url, attributes);
        }
    }

    private void assertSaveAttributesSuccess(String url, LeadEnrichmentAttribute[] attributes) {
        if (attributes.length > 1) {
            LeadEnrichmentAttribute[] attrsToSave = new LeadEnrichmentAttribute[attributes.length - 1];
            System.arraycopy(attributes, 1, attrsToSave, 0, attrsToSave.length);
            restTemplate.put(url, attrsToSave, new HashMap<>());
            LeadEnrichmentAttribute[] attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            assertEquals(attrsToSave.length, attrsSaved.length);
            restTemplate.put(url, attributes, new HashMap<>());
        } else {
            String avariableUrl = getRestAPIHostPort() + "/pls/leadenrichment/avariableattributes";
            LeadEnrichmentAttribute[] attrs = restTemplate.getForObject(avariableUrl, LeadEnrichmentAttribute[].class);
            if (attrs != null && attrs.length > 0) {
                LeadEnrichmentAttribute[] attrsToSave = null;
                for (LeadEnrichmentAttribute attr : attrs) {
                    if (!attr.getFieldName().equals(attributes[0].getFieldName())) {
                        attrsToSave = new LeadEnrichmentAttribute[2];
                        attrsToSave[0] = attributes[0];
                        attrsToSave[1] = attr;
                        break;
                    }
                }
                if (attrsToSave != null) {
                    restTemplate.put(url, attrsToSave, new HashMap<>());
                    LeadEnrichmentAttribute[] attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
                    assertEquals(attrsToSave.length, attrsSaved.length);
                    restTemplate.put(url, attributes, new HashMap<>());
                }
            }
        }
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
}
