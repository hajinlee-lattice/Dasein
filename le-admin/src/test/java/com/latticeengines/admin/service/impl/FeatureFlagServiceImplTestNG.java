package com.latticeengines.admin.service.impl;

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

    @Test(groups = "functional")
    public void testDefaultFeatureFlags() {
        FeatureFlagDefinitionMap defaultFeatureFlagMap = featureFlagService.getDefinitions();
        Assert.assertNotNull(defaultFeatureFlagMap);
        System.out.println(defaultFeatureFlagMap.keySet());
        Assert.assertTrue(defaultFeatureFlagMap.size() >= LatticeFeatureFlag.values().length,
                "Should have at least LatticeFeatureFlags");
        Assert.assertTrue(defaultFeatureFlagMap.size() >= LatticeFeatureFlag.values().length
                + PlsFeatureFlag.values().length, "Should have at least LatticeFeatureFlags and PlsFeatureFlags");
        FeatureFlagDefinition danteFeatureFlag = defaultFeatureFlagMap.get(LatticeFeatureFlag.DANTE.getName());
        Assert.assertNotNull(danteFeatureFlag);

        FeatureFlagDefinition quotaFeatureFlag = defaultFeatureFlagMap.get(LatticeFeatureFlag.QUOTA.getName());
        Assert.assertNotNull(quotaFeatureFlag);

        FeatureFlagDefinition targetMarketFeatureFlag = defaultFeatureFlagMap.get(LatticeFeatureFlag.TARGET_MARKET
                .getName());
        Assert.assertNotNull(targetMarketFeatureFlag);

        FeatureFlagDefinition createDefaultFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
        Assert.assertNotNull(createDefaultFeatureFlag);

        FeatureFlagDefinition enablePocTransformFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName());
        Assert.assertNotNull(enablePocTransformFeatureFlag);

        FeatureFlagDefinition useSalesforceSettingsFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_SALESFORCE_SETTINGS.getName());
        Assert.assertNotNull(useSalesforceSettingsFeatureFlag);

        FeatureFlagDefinition useMarketoSettingsFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_MARKETO_SETTINGS.getName());
        Assert.assertNotNull(useMarketoSettingsFeatureFlag);

        FeatureFlagDefinition useEloquaSettingsFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_ELOQUA_SETTINGS.getName());
        Assert.assertNotNull(useEloquaSettingsFeatureFlag);

        FeatureFlagDefinition allowPivotFileFeatureFlag = defaultFeatureFlagMap.get(LatticeFeatureFlag.ALLOW_PIVOT_FILE
                .getName());
        Assert.assertNotNull(allowPivotFileFeatureFlag);

        FeatureFlagDefinition useAccountMasterFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_ACCOUNT_MASTER.getName());
        Assert.assertNotNull(useAccountMasterFeatureFlag);

        FeatureFlagDefinition useDnbRtsAndModelingFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName());
        Assert.assertNotNull(useDnbRtsAndModelingFeatureFlag);

        FeatureFlagDefinition enableLatticeMarketoCredentialPageFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE.getName());
        Assert.assertNotNull(enableLatticeMarketoCredentialPageFeatureFlag);

        FeatureFlagDefinition enableInternalEnrichmentAttributesFeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());
        Assert.assertNotNull(enableInternalEnrichmentAttributesFeatureFlag);

        FeatureFlagDefinition enableDataProfilingV2FeatureFlag = defaultFeatureFlagMap
                .get(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName());
        Assert.assertNotNull(enableDataProfilingV2FeatureFlag);

        Assert.assertTrue(danteFeatureFlag.getConfigurable()
                && danteFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA)
                && danteFeatureFlag.getDisplayName() != null && danteFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(!quotaFeatureFlag.getConfigurable()
                && quotaFeatureFlag.getAvailableProducts().contains(LatticeProduct.PD)
                && quotaFeatureFlag.getDisplayName() != null && quotaFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(!targetMarketFeatureFlag.getConfigurable()
                && targetMarketFeatureFlag.getAvailableProducts().contains(LatticeProduct.PD)
                && targetMarketFeatureFlag.getDisplayName() != null
                && targetMarketFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(!createDefaultFeatureFlag.getConfigurable()
                && createDefaultFeatureFlag.getAvailableProducts().contains(LatticeProduct.PD)
                && createDefaultFeatureFlag.getDisplayName() != null
                && createDefaultFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(enablePocTransformFeatureFlag.getConfigurable()
                && enablePocTransformFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && enablePocTransformFeatureFlag.getDisplayName() != null
                && enablePocTransformFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(useSalesforceSettingsFeatureFlag.getConfigurable()
                && useSalesforceSettingsFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && useSalesforceSettingsFeatureFlag.getDisplayName() != null
                && useSalesforceSettingsFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(useMarketoSettingsFeatureFlag.getConfigurable()
                && useMarketoSettingsFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && useMarketoSettingsFeatureFlag.getDisplayName() != null
                && useMarketoSettingsFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(useEloquaSettingsFeatureFlag.getConfigurable()
                && useEloquaSettingsFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && useEloquaSettingsFeatureFlag.getDisplayName() != null
                && useEloquaSettingsFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(allowPivotFileFeatureFlag.getConfigurable()
                && allowPivotFileFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && allowPivotFileFeatureFlag.getDisplayName() != null
                && allowPivotFileFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(useAccountMasterFeatureFlag.getConfigurable()
                && useAccountMasterFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && useAccountMasterFeatureFlag.getDisplayName() != null
                && useAccountMasterFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(useDnbRtsAndModelingFeatureFlag.getConfigurable()
                && useDnbRtsAndModelingFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && useDnbRtsAndModelingFeatureFlag.getDisplayName() != null
                && useDnbRtsAndModelingFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(enableLatticeMarketoCredentialPageFeatureFlag.getConfigurable()
                && enableLatticeMarketoCredentialPageFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && enableLatticeMarketoCredentialPageFeatureFlag.getDisplayName() != null
                && enableLatticeMarketoCredentialPageFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(enableInternalEnrichmentAttributesFeatureFlag.getConfigurable()
                && enableInternalEnrichmentAttributesFeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && enableInternalEnrichmentAttributesFeatureFlag.getDisplayName() != null
                && enableInternalEnrichmentAttributesFeatureFlag.getDocumentation() != null);

        Assert.assertTrue(enableDataProfilingV2FeatureFlag.getConfigurable()
                && enableDataProfilingV2FeatureFlag.getAvailableProducts().contains(LatticeProduct.LPA3)
                && enableDataProfilingV2FeatureFlag.getDisplayName() != null
                && enableDataProfilingV2FeatureFlag.getDocumentation() != null);

    }

    @Test(groups = "functional")
    public void testDefineFlag() {

        featureFlagService.defineFlag(FLAG_ID, definition);
        FeatureFlagDefinition newDefinition = featureFlagService.getDefinition(FLAG_ID);
        Assert.assertNotNull(newDefinition);
        Assert.assertEquals(newDefinition.getDisplayName(), definition.getDisplayName());
        Assert.assertEquals(newDefinition.getDocumentation(), definition.getDocumentation());
        Assert.assertEquals(newDefinition.getAvailableProducts().size(), 3);
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
