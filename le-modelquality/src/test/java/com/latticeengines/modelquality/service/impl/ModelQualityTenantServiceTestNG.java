package com.latticeengines.modelquality.service.impl;

import java.lang.reflect.Field;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.ModelQualityTenantService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class ModelQualityTenantServiceTestNG extends ModelQualityFunctionalTestNGBase {
    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private ModelQualityTenantService modelQualityTenantService;

    private String testTenant = "testMQTenant";
    private String testUser = "testMQUser";

    @Override
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        User user = userService.findByUsername(testUser);
        if (user != null) {
            userService.deleteUser(testTenant, testUser);
        }

        Tenant tenant = tenantService.findByTenantName(testTenant);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    @Test(groups = "unit")
    public void testServiceImpl() throws Exception {
        Field field = ModelQualityTenantServiceImpl.class.getDeclaredField("tenant");
        field.setAccessible(true);
        field.set(modelQualityTenantService, testTenant);

        field = ModelQualityTenantServiceImpl.class.getDeclaredField("username");
        field.setAccessible(true);
        field.set(modelQualityTenantService, testUser);

        modelQualityTenantService.bootstrapServiceTenant();

        Assert.assertNotNull(tenantService.findByTenantName(testTenant));
        User user = userService.findByUsername(testUser);
        Assert.assertNotNull(user);
    }

    @AfterClass(groups = "unit")
    public void cleanup() {
        User user = userService.findByUsername(testUser);
        if (user != null) {
            userService.deleteUser(testTenant, testUser);
        }

        Tenant tenant = tenantService.findByTenantName(testTenant);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }
}
