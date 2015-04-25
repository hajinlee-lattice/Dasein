package com.latticeengines.security.exposed.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.security.exposed.service.UserService;

public class UserServiceImplUnitTestNG {

    private UserService userService = new UserServiceImpl();
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }
    
    @Test(groups = "unit")
    public void testGetURLSafeUsername() {
        String username = "build@lattice-engines.com";
        Assert.assertEquals(userService.getURLSafeUsername(username), "build@lattice-engines.com");

        username = "\"build@lattice-engines.com\"";
        Assert.assertEquals(userService.getURLSafeUsername(username), "build@lattice-engines.com");

        username = "a@b.c";
        Assert.assertEquals(userService.getURLSafeUsername(username), "a@b.c");

        username = "\"a@b.c\"";
        Assert.assertEquals(userService.getURLSafeUsername(username), "a@b.c");
    }

}
