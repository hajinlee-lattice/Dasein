package com.latticeengines.pls.service.impl;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.DefaultFeatureFlagProvider;
import com.latticeengines.pls.service.TenantConfigService;

public class TenantConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    private final static String contractId = "PLSTenantConfig";
    private final static String tenantId = contractId;
    private final static String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;
    private static String PLSTenantId;

    private final static String flagId = "PlsTestFlag";
    private final static String undefinedFlagId = "PlsTestFlagUndefined";
    private final static String noDefaultFlagId = "PlsTestFlagNoDefault";

    @Value("${pls.dataloader.rest.api}")
    private String defaultDataLoaderUrl;

    @Autowired
    private TenantConfigService configService;

    @Autowired
    @Qualifier("propertiesFileFeatureFlagProvider")
    private DefaultFeatureFlagProvider defaultFeatureFlagProvider;

    private BatonService batonService = new BatonServiceImpl();

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }

        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(contractId, tenantId, spaceId, spaceInfo);

        path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);


        path = path.append(new Path("/SpaceConfiguration"));
        if (!camille.exists(path)) {
            camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

        Path topologyPath = path.append(new Path("/Topology"));
        if (camille.exists(topologyPath)) { camille.delete(topologyPath); }
        camille.create(topologyPath, new Document("SFDC"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Path dlAddressPath = path.append(new Path("/DL_Address"));
        if (camille.exists(dlAddressPath)) { camille.delete(dlAddressPath); }
        camille.create(dlAddressPath, new Document(defaultDataLoaderUrl), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        PLSTenantId = String.format("%s.%s.%s", contractId, tenantId, spaceId);
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void getCredential() {
        CRMTopology topology = configService.getTopology(PLSTenantId);
        Assert.assertEquals(topology, CRMTopology.SFDC);
    }

    @Test(groups = "functional")
    public void getFeatureFlags() {
        removeFlagDefinition(flagId);
        removeFlagDefinition(noDefaultFlagId);

        FeatureFlagClient.setDefinition(flagId, new FeatureFlagDefinition());
        FeatureFlagClient.setDefinition(noDefaultFlagId, new FeatureFlagDefinition());

        FeatureFlagValueMap flags = configService.getFeatureFlags(PLSTenantId);
        verifyFlagMatchDefault(flags, flagId);
        verifyFlagMatchDefault(flags, noDefaultFlagId);
        verifyFlagMatchDefault(flags, undefinedFlagId);

        removeFlagDefinition(flagId);
        removeFlagDefinition(noDefaultFlagId);
    }

    @Test(groups = "functional")
    public void testOverwriteFeatureFlags() {
        removeFlagDefinition(flagId);
        removeFlagDefinition(noDefaultFlagId);

        FeatureFlagClient.setDefinition(flagId, new FeatureFlagDefinition());
        FeatureFlagClient.setDefinition(noDefaultFlagId, new FeatureFlagDefinition());

        // overwrite a flag
        testOverwriteFlag(flagId, true);
        testOverwriteFlag(flagId, false);
        testOverwriteFlag(noDefaultFlagId, true);
        testOverwriteFlag(noDefaultFlagId, false);

        removeFlagDefinition(flagId);
        removeFlagDefinition(noDefaultFlagId);
    }


    @Test(groups = "functional")
    public void testDataloaderRelatedFeatureFlags() throws Exception {
        FeatureFlagValueMap flags = configService.getFeatureFlags(PLSTenantId);
        verifyFlagBooleanAndDefault(flags, PlsFeatureFlag.ACTIVATE_MODEL_PAGE.getName(), true);
        verifyFlagBooleanAndDefault(flags, PlsFeatureFlag.SYSTEM_SETUP_PAGE.getName(), true);

        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);
        path = path.append(new Path("/SpaceConfiguration"));

        Path topologyPath = path.append(new Path("/Topology"));
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(topologyPath);

        flags = configService.getFeatureFlags(PLSTenantId);
        verifyFlagFalse(flags, PlsFeatureFlag.ACTIVATE_MODEL_PAGE.getName());
        verifyFlagFalse(flags, PlsFeatureFlag.SYSTEM_SETUP_PAGE.getName());

        camille.create(topologyPath, new Document("SFDC"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    private void testOverwriteFlag(String flagId, Boolean value) {
        CustomerSpace space = CustomerSpace.parse(PLSTenantId);
        FeatureFlagClient.setEnabled(space, flagId, value);
        FeatureFlagValueMap flags = configService.getFeatureFlags(PLSTenantId);
        Assert.assertEquals(flags.get(flagId), value);
    }

    private void verifyFlagMatchDefault(FeatureFlagValueMap flags, String flagId) {
        FeatureFlagValueMap defaultFlags = defaultFeatureFlagProvider.getDefaultFlags();
        Assert.assertEquals(flags.containsKey(flagId), defaultFlags.containsKey(flagId),
                String.format("%s exists in either tenant flags (%s) or default flags (%s), not both",
                        flagId, flags.containsKey(flagId), defaultFlags.containsKey(flagId)));
        if (flags.containsKey(flagId)) {
            Assert.assertEquals(flags.get(flagId), defaultFlags.get(flagId));
        }
    }

    @SuppressWarnings("unused")
    private void verifyFlagFalseOrNull(FeatureFlagValueMap flags, String flagId) {
        FeatureFlagValueMap defaultFlags = defaultFeatureFlagProvider.getDefaultFlags();
        if (defaultFlags.containsKey(flagId)) {
            Assert.assertFalse(flags.get(flagId));
        } else {
            Assert.assertNull(flags.get(flagId));
        }
    }

    private void verifyFlagFalse(FeatureFlagValueMap flags, String flagId) {
        Assert.assertTrue(flags.containsKey(flagId));
        Assert.assertFalse(flags.get(flagId));
    }

    private void verifyFlagBooleanAndDefault(FeatureFlagValueMap flags, String flagId, Boolean value) {
        FeatureFlagValueMap defaultFlags = defaultFeatureFlagProvider.getDefaultFlags();
        if (defaultFlags.containsKey(flagId)) {
            Boolean booleanAnd = defaultFlags.get(flagId) && value;
            Assert.assertEquals(flags.get(flagId), booleanAnd);
        } else {
            Assert.assertEquals(flags.get(flagId), value);
        }
    }

    private void removeFlagDefinition(String flagId) {
        try {
            FeatureFlagClient.remove(flagId);
        } catch (Exception e) {
            // ignore
        }
    }

}
