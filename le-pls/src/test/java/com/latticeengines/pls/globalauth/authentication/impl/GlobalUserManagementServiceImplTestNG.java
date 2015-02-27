package com.latticeengines.pls.globalauth.authentication.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;

public class GlobalUserManagementServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        UserDocument userDoc = loginAndAttach("admin");
        String tenant = userDoc.getTicket().getTenants().get(0).getId();
        createUser("abc", "abc@xyz.com", "Abc", "Def");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant, "abc");
    }

    @Test(groups = "functional")
    public void deleteUser() {
        UserDocument userDoc = loginAndAttach("abc");
        assertNotNull(userDoc);
        globalUserManagementService.deleteUser("abc");
        
        boolean exception = false;
        try {
            loginAndAttach("abc");
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);
    
      
    }
}
