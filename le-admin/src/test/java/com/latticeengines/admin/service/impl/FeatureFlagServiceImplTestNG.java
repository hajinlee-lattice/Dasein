package com.latticeengines.admin.service.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public class FeatureFlagServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private FeatureFlagService featureFlagService;

    private static FeatureFlagDefinition definition = newFlagDefinition();
    private static final String FLAG_ID = "TestFlag";

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "functional")
    public void testDefaultFeatureFlags() {
        FeatureFlagDefinitionMap defaultFeatureFlagMap = featureFlagService.getDefinitions();
        Assert.assertNotNull(defaultFeatureFlagMap);
        Assert.assertTrue(defaultFeatureFlagMap.size() >= LatticeFeatureFlag.values().length,
                "Should have at least LatticeFeatureFlags");
        Assert.assertTrue(
                defaultFeatureFlagMap.size() >= LatticeFeatureFlag.values().length + PlsFeatureFlag.values().length,
                "Should have at least LatticeFeatureFlags and PlsFeatureFlags");

        Collection<LatticeFeatureFlag> expectedPdFlags = Arrays.asList( //
                LatticeFeatureFlag.QUOTA, //
                LatticeFeatureFlag.TARGET_MARKET, //
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL);
        Collection<LatticeFeatureFlag> expectedCgFlags = Arrays.asList(//
                LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION, //
                LatticeFeatureFlag.ENABLE_CDL, //
                LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, //
                LatticeFeatureFlag.ENABLE_LPI_PLAYMAKER, //
                LatticeFeatureFlag.DANTE, //
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, //
                LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY, //
                LatticeFeatureFlag.SCORE_EXTERNAL_FILE, //
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH, //
                LatticeFeatureFlag.ENABLE_TARGET_SCORE_DERIVATION, //
                LatticeFeatureFlag.ENABLE_APS_IMPUTATION, //
                LatticeFeatureFlag.ENABLE_FILE_IMPORT, //
                LatticeFeatureFlag.ENABLE_CROSS_SELL_MODELING, //
                LatticeFeatureFlag.ENABLE_PRODUCT_PURCHASE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PRODUCT_BUNDLE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PRODUCT_HIERARCHY_IMPORT, //
                LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE,
                LatticeFeatureFlag.IMPORT_WITHOUT_ID,
                LatticeFeatureFlag.PLAYBOOK_MODULE, //
                LatticeFeatureFlag.LAUNCH_PLAY_TO_MAP_SYSTEM, //
                LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION, //
                LatticeFeatureFlag.ADVANCED_MODELING, //
                LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS, //
                LatticeFeatureFlag.MIGRATION_TENANT, //
                LatticeFeatureFlag.PROTOTYPE_FEATURE, //
                LatticeFeatureFlag.ALPHA_FEATURE, //
                LatticeFeatureFlag.BETA_FEATURE, //
                LatticeFeatureFlag.ENABLE_MULTI_TEMPLATE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PER_TENANT_MATCH_REPORT);
        Collection<LatticeFeatureFlag> expectedLp2Flags = Collections.singleton(LatticeFeatureFlag.DANTE);
        Collection<LatticeFeatureFlag> expectedNonLpiFlags = new HashSet<>();
        Collection<LatticeFeatureFlag> expectedDefaultFalseFlags = Arrays.asList( //
                LatticeFeatureFlag.ALLOW_PIVOT_FILE, //
                LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, //
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, //
                LatticeFeatureFlag.QUOTA, //
                LatticeFeatureFlag.TARGET_MARKET, //
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL, //
                LatticeFeatureFlag.BYPASS_DNB_CACHE, //
                LatticeFeatureFlag.ENABLE_MATCH_DEBUG, //
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH, //
                LatticeFeatureFlag.ENABLE_APS_IMPUTATION, //
                LatticeFeatureFlag.VDB_MIGRATION, //
                LatticeFeatureFlag.SCORE_EXTERNAL_FILE, //
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, //
                LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY, //
                LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE,
                LatticeFeatureFlag.IMPORT_WITHOUT_ID,
                LatticeFeatureFlag.LAUNCH_PLAY_TO_MAP_SYSTEM, //
                LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION, //
                LatticeFeatureFlag.ADVANCED_MODELING, //
                LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS, //
                LatticeFeatureFlag.MIGRATION_TENANT, //
                LatticeFeatureFlag.PROTOTYPE_FEATURE, //
                LatticeFeatureFlag.ALPHA_FEATURE, //
                LatticeFeatureFlag.BETA_FEATURE, //
                LatticeFeatureFlag.ENABLE_MULTI_TEMPLATE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PER_TENANT_MATCH_REPORT);
        expectedNonLpiFlags.addAll(expectedLp2Flags);
        expectedNonLpiFlags.addAll(expectedPdFlags);
        expectedNonLpiFlags.addAll(expectedCgFlags);
        expectedNonLpiFlags.remove(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION);

        for (LatticeFeatureFlag latticeFeatureFlag : LatticeFeatureFlag.values()) {
            FeatureFlagDefinition flagDefinition = defaultFeatureFlagMap.get(latticeFeatureFlag.getName());
            Assert.assertNotNull(flagDefinition,
                    "Did not find definition for lattice feature flag " + latticeFeatureFlag.getName());
            Assert.assertNotNull(flagDefinition.getDisplayName(),
                    "Lattice feature flag " + latticeFeatureFlag.getName() + " has empty display name");
            Assert.assertNotNull(flagDefinition.getDocumentation(),
                    "Lattice feature flag " + latticeFeatureFlag.getName() + " has empty documentation");

            if (!expectedNonLpiFlags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getAvailableProducts().contains(LatticeProduct.LPA3),
                        latticeFeatureFlag.getName() + " should be included in the product "
                                + LatticeProduct.LPA3.getName() + ", but it is not.");
            }

            if (expectedPdFlags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getAvailableProducts().contains(LatticeProduct.PD),
                        latticeFeatureFlag.getName() + " should be included in the product "
                                + LatticeProduct.PD.getName() + ", but it is not.");
            }

            if (expectedLp2Flags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getAvailableProducts().contains(LatticeProduct.LPA),
                        latticeFeatureFlag.getName() + " should be included in the product "
                                + LatticeProduct.LPA.getName() + ", but it is not.");
            }

            if (expectedCgFlags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getAvailableProducts().contains(LatticeProduct.CG),
                        latticeFeatureFlag.getName() + " should be included in the product "
                                + LatticeProduct.CG.getName() + ", but it is not.");
            }
            if (!expectedDefaultFalseFlags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getDefaultValue(),
                        String.format("Default feature flag %s, value should be true", latticeFeatureFlag.getName()));
            } else {
                Assert.assertFalse(flagDefinition.getDefaultValue(),
                        String.format("Default feature flag %s, value should be false", latticeFeatureFlag.getName()));
            }
        }
    }

    @Test(groups = "functional")
    public void testDefineFlag() {
        featureFlagService.defineFlag(FLAG_ID, definition);
        FeatureFlagDefinition newDefinition = featureFlagService.getDefinition(FLAG_ID);
        Assert.assertNotNull(newDefinition);
        Assert.assertEquals(newDefinition.getDisplayName(), definition.getDisplayName());
        Assert.assertEquals(newDefinition.getDocumentation(), definition.getDocumentation());
        Assert.assertEquals(newDefinition.getAvailableProducts().size(), 4);
        Assert.assertTrue(newDefinition.getAvailableProducts().contains(LatticeProduct.LPA));
        Assert.assertTrue(newDefinition.getAvailableProducts().contains(LatticeProduct.LPA3));
        Assert.assertTrue(newDefinition.getAvailableProducts().contains(LatticeProduct.PD));
        Assert.assertTrue(newDefinition.getConfigurable());

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

        featureFlagService.removeFlag(TestTenantId, FLAG_ID);
        flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertFalse(flags.containsKey(FLAG_ID), "TestFlag should have been removed.");

        featureFlagService.undefineFlag(FLAG_ID);
        definition.setConfigurable(false);
        featureFlagService.defineFlag(FLAG_ID, definition);
        try {
            featureFlagService.setFlag(TestTenantId, FLAG_ID, true);
        } catch (Exception e) {
            Assert.fail("TestFlag should have been set even it not configurable, since configurability"
                    + "only has something to do with the UI in tenant console");
        }
        flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertTrue(flags.containsKey(FLAG_ID), "TestFlag should be defined.");
        Assert.assertTrue(flags.get(FLAG_ID), "TestFlag should have been set.");
    }

}
