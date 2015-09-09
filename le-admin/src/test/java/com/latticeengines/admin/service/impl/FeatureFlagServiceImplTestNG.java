package com.latticeengines.admin.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpException;

public class FeatureFlagServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private FeatureFlagService featureFlagService;

    private static final FeatureFlagDefinition definition = newFlagDefinition();
    private static final String FLAG_ID = "TestFlag";

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @Test(groups = "functional")
    public void testDefineFlag() {
        featureFlagService.defineFlag(FLAG_ID, definition);
        FeatureFlagDefinition newDefinition = featureFlagService.getDefinition(FLAG_ID);
        Assert.assertNotNull(newDefinition);
        Assert.assertEquals(newDefinition.displayName, definition.displayName);
        Assert.assertEquals(newDefinition.documentation, definition.documentation);

        boolean encounterException = false;
        try {
            featureFlagService.defineFlag(FLAG_ID, definition);
        } catch (LedpException e) {
            encounterException = true;

        }
        Assert.assertTrue(encounterException, "Defining an existing flag should raise exception.");
    }

    @Test(groups = "functional")
    public void testGetDefinitions() {
        FeatureFlagDefinitionMap originalMap = featureFlagService.getDefinitions();
        Assert.assertTrue(originalMap.size() >= 0);

        featureFlagService.defineFlag(FLAG_ID, definition);

        FeatureFlagDefinitionMap newMap = featureFlagService.getDefinitions();
        Assert.assertEquals(newMap.size(), originalMap.size() + 1);
    }

    @Test(groups = "functional")
    public void testSetFlag() {
        featureFlagService.defineFlag(FLAG_ID, definition);
        FeatureFlagValueMap flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertFalse(flags.containsKey(FLAG_ID), "TestFlag should have not been set.");

        featureFlagService.setFlag(TestTenantId, FLAG_ID, true);
        flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertTrue(flags.containsKey(FLAG_ID), "TestFlag should have been set.");
        Assert.assertTrue(flags.get(FLAG_ID));


        featureFlagService.removeFlag(TestTenantId, FLAG_ID);
        flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertFalse(flags.containsKey(FLAG_ID), "TestFlag should have been removed.");

        featureFlagService.setFlag(TestTenantId, FLAG_ID, false);
        flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertTrue(flags.containsKey(FLAG_ID), "TestFlag should have been set.");
        Assert.assertFalse(flags.get(FLAG_ID));
    }

    private static FeatureFlagDefinition newFlagDefinition() {
        FeatureFlagDefinition definition = new FeatureFlagDefinition();
        definition.displayName = "TestFlag";
        definition.documentation = "This flag is for functional test.";
        return definition;
    }
}
