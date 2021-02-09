package com.latticeengines.admin.service.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public class FeatureFlagServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Inject
    private FeatureFlagService featureFlagService;

    @Inject
    private TenantService tenantService;

    private static FeatureFlagDefinition definition = newFlagDefinition();
    private static final String FLAG_ID = "TestFlag";
    private static final String FLAG2_ID = "TestFlag2";

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

        Collection<LatticeFeatureFlag> expectedPdFlags = EnumSet.of( //
                LatticeFeatureFlag.QUOTA, //
                LatticeFeatureFlag.TARGET_MARKET, //
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL);
        Collection<LatticeFeatureFlag> expectedCgFlags = EnumSet.of(//
                LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION, //
                LatticeFeatureFlag.ENABLE_CDL, //
                LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, //
                LatticeFeatureFlag.ENABLE_LPI_PLAYMAKER, //
                LatticeFeatureFlag.DANTE, //
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, //
                LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY, //
                LatticeFeatureFlag.SCORE_EXTERNAL_FILE, //
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH, //
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA, //
                LatticeFeatureFlag.ENABLE_TARGET_SCORE_DERIVATION, //
                LatticeFeatureFlag.ENABLE_APS_IMPUTATION, //
                LatticeFeatureFlag.ENABLE_FILE_IMPORT, //
                LatticeFeatureFlag.ENABLE_CROSS_SELL_MODELING, //
                LatticeFeatureFlag.ENABLE_PRODUCT_PURCHASE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PRODUCT_BUNDLE_IMPORT, //
                LatticeFeatureFlag.ENABLE_PRODUCT_HIERARCHY_IMPORT, //
                LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE, //
                LatticeFeatureFlag.IMPORT_WITHOUT_ID, //
                LatticeFeatureFlag.PLAYBOOK_MODULE, //
                LatticeFeatureFlag.LAUNCH_PLAY_TO_MAP_SYSTEM, //
                LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_FACEBOOK_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_LINKEDIN_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_OUTREACH_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_GOOGLE_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_LIVERAMP_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_IR_DEFAULT_IDS, //
                LatticeFeatureFlag.ENABLE_DELTA_CALCULATION, //
                LatticeFeatureFlag.ENABLE_ACCOUNT360, //
                LatticeFeatureFlag.ADVANCED_MODELING, //
                LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS, //
                LatticeFeatureFlag.MIGRATION_TENANT, //
                LatticeFeatureFlag.PROTOTYPE_FEATURE, //
                LatticeFeatureFlag.ALPHA_FEATURE, //
                LatticeFeatureFlag.BETA_FEATURE, //
                LatticeFeatureFlag.ATTRIBUTE_TOGGLING, //
                LatticeFeatureFlag.ADVANCED_FILTERING, //
                LatticeFeatureFlag.ENABLE_ACXIOM, //
                LatticeFeatureFlag.ENABLE_IMPORT_V2, //
                LatticeFeatureFlag.SSVI_REPORT,
                LatticeFeatureFlag.ENABLE_IMPORT_ERASE_BY_NULL,
                LatticeFeatureFlag.PUBLISH_TO_ELASTICSEARCH, //
                LatticeFeatureFlag.QUERY_FROM_ELASTICSEARCH
        );
        Collection<LatticeFeatureFlag> expectedLp2Flags = Collections.singleton(LatticeFeatureFlag.DANTE);
        Collection<LatticeFeatureFlag> expectedNonLpiFlags = EnumSet.noneOf(LatticeFeatureFlag.class);
        Collection<LatticeFeatureFlag> expectedDcpFlags = EnumSet.of(
                LatticeFeatureFlag.DCP_ENRICHMENT_LIBRARY,
                LatticeFeatureFlag.MATCH_MAPPING_V2
        );

        Collection<LatticeFeatureFlag> expectedDefaultFalseFlags = EnumSet.of( //
                LatticeFeatureFlag.ALLOW_PIVOT_FILE, //
                LatticeFeatureFlag.ENABLE_CAMPAIGN_UI, //
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, //
                LatticeFeatureFlag.QUOTA, //
                LatticeFeatureFlag.TARGET_MARKET, //
                LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL, //
                LatticeFeatureFlag.BYPASS_DNB_CACHE, //
                LatticeFeatureFlag.ENABLE_MATCH_DEBUG, //
                LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA, //
                LatticeFeatureFlag.VDB_MIGRATION, //
                LatticeFeatureFlag.SCORE_EXTERNAL_FILE, //
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE, //
                LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE, //
                LatticeFeatureFlag.IMPORT_WITHOUT_ID, //
                LatticeFeatureFlag.LAUNCH_PLAY_TO_MAP_SYSTEM, //
                LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_FACEBOOK_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_OUTREACH_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_GOOGLE_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_LIVERAMP_INTEGRATION, //
                LatticeFeatureFlag.ENABLE_IR_DEFAULT_IDS, //
                LatticeFeatureFlag.ENABLE_ACCOUNT360, //
                LatticeFeatureFlag.ADVANCED_MODELING, //
                LatticeFeatureFlag.MIGRATION_TENANT, //
                LatticeFeatureFlag.PROTOTYPE_FEATURE, //
                LatticeFeatureFlag.ALPHA_FEATURE, //
                LatticeFeatureFlag.BETA_FEATURE, //
                LatticeFeatureFlag.ATTRIBUTE_TOGGLING, //
                LatticeFeatureFlag.ENABLE_ACXIOM, //
                LatticeFeatureFlag.ENABLE_IMPORT_V2,
                LatticeFeatureFlag.SSVI_REPORT,
                LatticeFeatureFlag.ENABLE_IMPORT_ERASE_BY_NULL,
                LatticeFeatureFlag.PUBLISH_TO_ELASTICSEARCH,
                LatticeFeatureFlag.QUERY_FROM_ELASTICSEARCH,
                LatticeFeatureFlag.DCP_ENRICHMENT_LIBRARY,
                LatticeFeatureFlag.MATCH_MAPPING_V2
        );

        expectedNonLpiFlags.addAll(expectedLp2Flags);
        expectedNonLpiFlags.addAll(expectedPdFlags);
        expectedNonLpiFlags.addAll(expectedCgFlags);
        expectedNonLpiFlags.addAll(expectedDcpFlags);
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

            if (expectedDcpFlags.contains(latticeFeatureFlag)) {
                Assert.assertTrue(flagDefinition.getAvailableProducts().contains(LatticeProduct.DCP),
                        latticeFeatureFlag.getName() + " should be included in the product "
                                + LatticeProduct.DCP.getName() + ", but it is not.");
            }

            Assert.assertNotEquals(expectedDefaultFalseFlags.contains(latticeFeatureFlag), flagDefinition.getDefaultValue(),
                    String.format("Default feature flag %s, value should be %s", latticeFeatureFlag.getName(), !expectedDefaultFalseFlags.contains(latticeFeatureFlag)));
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

    @Test(groups = "functional")
    public void testDefaultTrueFlag() {
        FeatureFlagDefinition definition = newFlagDefinitionWithoutLPA();
        featureFlagService.undefineFlag(FLAG2_ID);

        featureFlagService.defineFlag(FLAG2_ID, definition);
        FeatureFlagValueMap flags = featureFlagService.getFlags(TestTenantId);
        Assert.assertFalse(flags.containsKey(FLAG2_ID), "TestFlag should have not been set.");

        CustomerSpace customerSpace = CustomerSpace.parse(TestTenantId);
        TenantDocument tenantDocument = tenantService.getTenant(customerSpace.getContractId(),
                customerSpace.getTenantId());
        FeatureFlagValueMap ffMap = tenantDocument.getFeatureFlags();
        // There's an update for default flag PLS-18931:
        // 1. Before update: if a flag not belonging to current products, it will always return it's default value.
        // 2. After update: if a flag not belonging to current products, it will be removed from returned feature flag map.
        Assert.assertNull(ffMap.get(FLAG2_ID));

        featureFlagService.undefineFlag(FLAG2_ID);
    }

    protected static FeatureFlagDefinition newFlagDefinitionWithoutLPA() {
        FeatureFlagDefinition definition = new FeatureFlagDefinition();
        definition.setDisplayName(FLAG2_ID);
        definition.setDocumentation("This flag is for functional test.");
        Set<LatticeProduct> testProdSet = new HashSet<>();
        testProdSet.add(LatticeProduct.LPA3);
        testProdSet.add(LatticeProduct.CG);
        definition.setAvailableProducts(testProdSet);
        definition.setConfigurable(true);
        definition.setDefaultValue(true);
        return definition;
    }

}
