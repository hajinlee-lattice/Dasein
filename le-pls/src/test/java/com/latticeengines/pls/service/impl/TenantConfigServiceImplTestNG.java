package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.List;

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
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.DefaultFeatureFlagProvider;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.TenantDeploymentConstants;
import com.latticeengines.pls.service.TenantDeploymentService;
import com.latticeengines.pls.util.ValidateEnrichAttributesUtils;

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
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

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

        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        spaceConfiguration.setDlAddress(defaultDataLoaderUrl);
        spaceConfiguration.setProducts(Collections.singletonList(LatticeProduct.LPA3));
        spaceConfiguration.setTopology(CRMTopology.SFDC);
        batonService.setupSpaceConfiguration(contractId, tenantId, spaceId, spaceConfiguration);

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

        FeatureFlagDefinition f1 = new FeatureFlagDefinition();
        f1.setConfigurable(true);
        FeatureFlagDefinition f2 = new FeatureFlagDefinition();
        f2.setConfigurable(true);
        FeatureFlagClient.setDefinition(flagId, f1);
        FeatureFlagClient.setDefinition(noDefaultFlagId, f2);

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

    @Test(groups = "functional")
    public void testDeploymentRelatedFeatureFlags() throws Exception {
        FeatureFlagValueMap flags = configService.getFeatureFlags(PLSTenantId);
        String flagId = PlsFeatureFlag.DEPLOYMENT_WIZARD_PAGE.getName();
        CRMTopology topology = configService.getTopology(PLSTenantId);
        if (CRMTopology.SFDC.getName().equals(topology.getName())) {
            verifyFlagBooleanAndDefault(flags, flagId, true);
            boolean redirect = redirectDeploymentWizardPage(PLSTenantId);
            verifyFlagBooleanAndDefault(flags, TenantDeploymentConstants.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, redirect);
        } else {
            verifyFlagBooleanAndDefault(flags, flagId, false);
            verifyFlagBooleanAndDefault(flags, TenantDeploymentConstants.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, false);
        }
    }

    @Test(groups = "functional")
    public void getProducts() {
        List<LatticeProduct> products = configService.getProducts(PLSTenantId);
        Assert.assertNotNull(products);
        Assert.assertEquals(products.get(0), LatticeProduct.LPA3);

        products = configService.getProducts("nope");
        Assert.assertTrue(products.isEmpty());
    }

    private boolean redirectDeploymentWizardPage(String tenantId) {
        try {
            TenantDeployment tenantDeployment = tenantDeploymentService.getTenantDeployment(tenantId);
            if (tenantDeployment != null) {
                return !tenantDeploymentService.isDeploymentCompleted(tenantDeployment);
            } else {
                List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
                if (summaries != null) {
                    for (ModelSummary summary : summaries) {
                        if (modelSummaryService.modelIdinTenant(summary.getId(), tenantId)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Test(groups = "functional")
    public void testRemoveDLRestServicePart() {
        String url = "https://10.41.1.187:8081/DLRestService";
        String newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "https://10.41.1.187:8081/DLRestService/";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "https://10.41.1.187:8081/dlrestservice";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "https://10.41.1.187:8081/dlrestservice/";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "https://10.41.1.187:8081/";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "https://10.41.1.187:8081";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "https://10.41.1.187:8081");

        url = "/";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "");

        url = "";
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, "");

        url = null;
        newUrl = configService.removeDLRestServicePart(url);
        Assert.assertEquals(newUrl, null);
    }

    @Test(groups = "functional")
    public void testGetMaxPremiumLeadEnrichmentAttributes() {
        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath;
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        contractPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId()).append(
                new Path(TenantConfigServiceImpl.SERVICES_ZNODE + TenantConfigServiceImpl.PLS_ZNODE
                        + TenantConfigServiceImpl.ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE));
        logger.info("The contractPath is " + contractPath);
        try {
            camille.delete(contractPath);
        } catch (Exception ex) {
            logger.error("Error cleaning up the zookeeper node.");
        }

        Assert.assertEquals(configService.getMaxPremiumLeadEnrichmentAttributes(tenantId),
                ValidateEnrichAttributesUtils.DEFAULT_PREMIUM_ENRICHMENT_ATTRIBUTES);
        try {
            camille.upsert(contractPath, DocumentUtils.toRawDocument(12), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            logger.error("Error modifying the zookeeper node.");
        }
        Assert.assertEquals(configService.getMaxPremiumLeadEnrichmentAttributes(tenantId), 12);
    }

    private void testOverwriteFlag(String flagId, Boolean value) {
        CustomerSpace space = CustomerSpace.parse(PLSTenantId);
        FeatureFlagClient.setEnabled(space, flagId, value);
        FeatureFlagValueMap flags = configService.getFeatureFlags(PLSTenantId);
        Assert.assertEquals(flags.get(flagId), value);
    }

    private void verifyFlagMatchDefault(FeatureFlagValueMap flags, String flagId) {
        FeatureFlagValueMap defaultFlags = defaultFeatureFlagProvider.getDefaultFlags();
        Assert.assertEquals(
                flags.containsKey(flagId), //
                defaultFlags.containsKey(flagId), //
                String.format("%s exists in either tenant flags (%s) or default flags (%s), not both", flagId,
                        flags.containsKey(flagId), defaultFlags.containsKey(flagId)));
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
