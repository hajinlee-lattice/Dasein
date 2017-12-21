package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantConfigEntityMgr;
import com.latticeengines.auth.exposed.repository.GlobalAuthUserTenantConfigRepository;
import com.latticeengines.auth.testframework.AuthFunctionalTestNGBase;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantConfig;
import com.latticeengines.domain.exposed.auth.UserConfigProperty;

public class GlobalAuthUserTenantConfigEntityMgrTestNG extends AuthFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthUserTenantConfigEntityMgrTestNG.class);

    @Autowired
    protected GlobalAuthUserTenantConfigEntityMgr gaUserTenantConfigEntityMgr;

    protected GlobalAuthUser gaUser;

    protected GlobalAuthTenant gaTenant;
    protected GlobalAuthTenant gaTenant2;

    @Autowired
    private GlobalAuthUserTenantConfigRepository gaUserTenantConfigRepository;

    @BeforeClass
    public void setup() {
        gaTenant = createGlobalAuthTenant();
        Assert.assertNotNull(gaTenant.getPid());

        gaUser = createGlobalAuthUser();
        Assert.assertNotNull(gaUser.getPid());
    }

    @Test(groups = "functional")
    public void testCreateUserConfig() {
        GlobalAuthUserTenantConfig userConfig = createUserConfig(UserConfigProperty.SSO_ENABLED,
                Boolean.TRUE.toString(), gaTenant, gaUser);
        Assert.assertNotNull(userConfig.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateUserConfig" })
    public void testCreateUserConfig2() {
        GlobalAuthUserTenantConfig userConfig = createUserConfig(UserConfigProperty.FORCE_SSO_LOGIN,
                Boolean.TRUE.toString(), gaTenant, gaUser);
        Assert.assertNotNull(userConfig.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateUserConfig2" })
    public void testFindUserConfig() {
        List<GlobalAuthUserTenantConfig> userConfigs = gaUserTenantConfigEntityMgr
                .findByUserIdAndTenantId(gaUser.getPid(), gaTenant.getPid());
        Assert.assertNotNull(userConfigs);
        Assert.assertTrue(userConfigs.size() == 2);
        userConfigs = gaUserTenantConfigEntityMgr.findByUserIdAndTenantId(null, null);
        Assert.assertNotNull(userConfigs);
        Assert.assertTrue(userConfigs.size() == 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateUserConfig2" })
    public void testCreateAndFindSsoEnabledTenants() {
        gaTenant2 = createGlobalAuthTenant();
        Assert.assertNotNull(gaTenant2.getPid());

        GlobalAuthUserTenantConfig userConfig = createUserConfig(UserConfigProperty.SSO_ENABLED,
                Boolean.TRUE.toString(), gaTenant2, gaUser);
        Assert.assertNotNull(userConfig.getPid());
        userConfig = createUserConfig(UserConfigProperty.FORCE_SSO_LOGIN, Boolean.FALSE.toString(), gaTenant2, gaUser);
        Assert.assertNotNull(userConfig.getPid());

        List<GlobalAuthUserTenantConfig> userConfigs = gaUserTenantConfigEntityMgr
                .findByUserIdAndTenantId(gaUser.getPid(), gaTenant2.getPid());
        Assert.assertNotNull(userConfigs);
        Assert.assertTrue(userConfigs.size() == 2);

        userConfigs = gaUserTenantConfigEntityMgr.findByUserId(gaUser.getPid());
        Assert.assertNotNull(userConfigs);
        Assert.assertTrue(userConfigs.size() == 4);

        List<GlobalAuthUserConfigSummary> userConfigSummaries = gaUserTenantConfigEntityMgr
                .findUserConfigSummaryByUserId(gaUser.getPid());
        Assert.assertNotNull(userConfigSummaries);
        Assert.assertTrue(userConfigSummaries.size() == 2);
        System.out.println("Size:" + userConfigs.size());

        GlobalAuthUserConfigSummary tenant1Summary = null;
        GlobalAuthUserConfigSummary tenant2Summary = null;
        for (GlobalAuthUserConfigSummary row : userConfigSummaries) {
            if (gaTenant.getId().equals(row.getTenantDeploymentId())) {
                tenant1Summary = row;
            } else if (gaTenant2.getId().equals(row.getTenantDeploymentId())) {
                tenant2Summary = row;
            }
        }

        Assert.assertNotNull(tenant1Summary);
        Assert.assertTrue(tenant1Summary.getSsoEnabled());
        Assert.assertTrue(tenant1Summary.getForceSsoLogin());

        Assert.assertNotNull(tenant2Summary);
        Assert.assertTrue(tenant2Summary.getSsoEnabled());
        Assert.assertFalse(tenant2Summary.getForceSsoLogin());

        tenant2Summary = gaUserTenantConfigEntityMgr.findUserConfigSummaryByUserIdTenantId(gaUser.getPid(),
                gaTenant2.getPid());
        Assert.assertNotNull(tenant2Summary);
        Assert.assertTrue(tenant2Summary.getSsoEnabled());
        Assert.assertFalse(tenant2Summary.getForceSsoLogin());

        tenant2Summary = gaUserTenantConfigEntityMgr.findUserConfigSummaryByUserIdTenantId(gaUser.getPid(),
                gaTenant2.getPid() + 100);
        Assert.assertNull(tenant2Summary);
    }

    protected GlobalAuthUserTenantConfig createUserConfig(UserConfigProperty prop, String value,
            GlobalAuthTenant gTenant, GlobalAuthUser gUser) {
        GlobalAuthUserTenantConfig userConfig = new GlobalAuthUserTenantConfig();
        userConfig.setConfigProperty(prop.name());
        userConfig.setPropertyValue(value);

        userConfig.setGlobalAuthTenant(gTenant);
        userConfig.setGlobalAuthUser(gUser);
        gaUserTenantConfigEntityMgr.create(userConfig);
        return userConfig;
    }
}
