package com.latticeengines.pls.controller;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.net.URI;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.springframework.web.util.UriComponentsBuilder;

import com.latticeengines.domain.exposed.pls.Contact;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ContactResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_CONTACT_URL = "pls/contacts/";
    private static final String PLS_COUNT_URL = "pls/contacts/count/";
    private static final String TEST_CONTACT_ID = "4";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
    }

    @Test(groups = { "functional" })
    public void findById() {

        Contact contact = restTemplate.getForObject(getRestAPIHostPort()
                + PLS_CONTACT_URL + TEST_CONTACT_ID, Contact.class);
        assertNotNull(contact);
        assertEquals(contact.getId(), TEST_CONTACT_ID);

    }

    @Test(groups = { "functional" })
    public void findByCriterias() {
        URI contactsUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_CONTACT_URL)
            .queryParam("Page", "0")
            .queryParam("Size", "16")
            .queryParam("CompanyID", "123#234")
            .queryParam("JobType", "Marketing")
            .build()
            .toUri();
        List contacts = restTemplate.getForObject(contactsUrl, List.class);

        URI countUrl= UriComponentsBuilder.fromUriString(getRestAPIHostPort()
                + PLS_COUNT_URL)
            .queryParam("CompanyID", "123#234")
            .queryParam("JobType", "Marketing")
            .build()
            .toUri();
        Long count = restTemplate.getForObject(countUrl, Long.class);
        assertNotNull(contacts);
        assertTrue(contacts.size() == 4);
        assertTrue(count == 4);

        for (int index = 0; index < contacts.size(); index++) {
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map) contacts.get(index);
            assertEquals(map.get("JobType"), "Marketing");
        }
    }

}
