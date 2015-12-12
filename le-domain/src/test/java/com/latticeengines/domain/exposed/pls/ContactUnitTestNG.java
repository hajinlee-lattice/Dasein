package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ContactUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        Contact contact = new Contact();
        
        contact.setPid(new Long(123));
        contact.setId("123");
        contact.setCompanyId(new Long(123));
        contact.setFirstName("abc");
        contact.setLastName("bcd");
        contact.setJobType("dcf");
        contact.setJobLevel("fgh");
        contact.setTitles("ghi");
        contact.setEmail("hij@hij.com");
        contact.setPhone("123-456789");

        String serializedStr = contact.toString();
        System.out.println(serializedStr);
        Contact deserializedContact = JsonUtils.deserialize(serializedStr, Contact.class);
        
        assertEquals(deserializedContact.getId(), contact.getId());
        assertEquals(deserializedContact.getFirstName(), contact.getFirstName());
        assertEquals(deserializedContact.getEmail(), contact.getEmail());
        assertEquals(deserializedContact.getJobType(), contact.getJobType());
        assertEquals(deserializedContact.getPhone(), contact.getPhone());
    }
}
