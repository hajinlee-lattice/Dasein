package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Test(groups = "deployment")
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
        String avariableUrl = getRestAPIHostPort() + "/pls/leadenrichment/avariableattributes";
        LeadEnrichmentAttribute[] avariableAttributes = restTemplate.getForObject(avariableUrl, LeadEnrichmentAttribute[].class);
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
        if (attributes != null && attributes.length > 0) {
            LeadEnrichmentAttribute[] attrsToSave = new LeadEnrichmentAttribute[0];
            restTemplate.put(url, attrsToSave, new HashMap<>());
            LeadEnrichmentAttribute[] attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            assertEquals(attrsToSave.length, attrsSaved.length);
            restTemplate.put(url, attributes, new HashMap<>());
        } else {
            LeadEnrichmentAttribute[] attrsToSave = new LeadEnrichmentAttribute[1];
            attrsToSave[0] = avariableAttributes[0];
            restTemplate.put(url, attrsToSave, new HashMap<>());
            LeadEnrichmentAttribute[] attrsSaved = restTemplate.getForObject(url, LeadEnrichmentAttribute[].class);
            assertEquals(attrsToSave.length, attrsSaved.length);
            restTemplate.put(url, attributes, new HashMap<>());
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
