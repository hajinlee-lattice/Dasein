package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Contact;
import com.latticeengines.pls.entitymanager.ContactEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ContactEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private ContactEntityMgr contactEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        // Nothing to be done right now. Don't want mess with AccountMaster.
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        // Nothing to be done right now. Don't want mess with AccountMaster.
    }

    @Test(groups = "functional")
    public void findByContactId() {
        Contact contact = contactEntityMgr.findById("4");
        assertNotNull(contact);
        assertEquals(contact.getId(), "4");
    }

    @Test(groups = "functional")
    public void findByCriterias() {
        Map<String, String> criterias = new HashMap<String, String>();
        criterias.put("Page", "0");
        criterias.put("Size", "16");
        criterias.put("CompanyID", "123");
        criterias.put("JobType", "Marketing");
        List<Contact> contacts = contactEntityMgr.findContacts(criterias);
        Long count = contactEntityMgr.findContactCount(criterias);
        assertNotNull(contacts);
        assertTrue(contacts.size() == 3);
        assertTrue(count.longValue() == 3);
        for (int index = 0; index < contacts.size(); index++) {
            Contact contact = (Contact)contacts.get(index);
            assertEquals(contact.getCompanyId(), new Long(123));
            assertEquals(contact.getJobType(), new String("Marketing"));
        }
    }

}
