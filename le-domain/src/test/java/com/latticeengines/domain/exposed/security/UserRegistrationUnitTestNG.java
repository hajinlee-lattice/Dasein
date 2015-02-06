package com.latticeengines.domain.exposed.security;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class UserRegistrationUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        UserRegistration userReg = new UserRegistration();
        
        User user = new User();
        user.setEmail("hford@ford.com");
        user.setFirstName("Henry");
        user.setLastName("Ford");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("CEO");
        
        Credentials creds = new Credentials();
        creds.setUsername("hford");
        creds.setPassword("changeme");
        
        Tenant tenant = new Tenant();
        tenant.setId("Ford");
        tenant.setName("Ford");
        
        userReg.setUser(user);
        userReg.setCredentials(creds);
        userReg.setTenant(tenant);
        
        String serializedStr = userReg.toString();
        UserRegistration deserializedUserReg = JsonUtils.deserialize(serializedStr, UserRegistration.class);
        
        assertEquals(deserializedUserReg.getUser().toString(), userReg.getUser().toString());
        assertEquals(deserializedUserReg.getCredentials().toString(), userReg.getCredentials().toString());
        assertEquals(deserializedUserReg.getTenant().toString(), userReg.getTenant().toString());
    }
}
