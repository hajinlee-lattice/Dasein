package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.DefaultFeatureFlagProvider;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.TenantDeploymentConstants;
import com.latticeengines.pls.service.TenantDeploymentService;

@Component("tenantConfigService")
public class TenantConfigServiceImpl implements TenantConfigService {

    private static final Log log = LogFactory.getLog(TenantConfigServiceImpl.class);
    private static final BatonService batonService = new BatonServiceImpl();
    private static final String SPACE_CONFIGURATION_ZNODE = "/SpaceConfiguration";
    private static final String TOPOLOGY_ZNODE = "/Topology";
    private static final String DL_ADDRESS_ZNODE = "/DL_Address";

    @Value("${pls.dataloader.rest.api}")
    private String defaultDataLoaderUrl;

    @Autowired
    @Qualifier("propertiesFileFeatureFlagProvider")
    private DefaultFeatureFlagProvider defaultFeatureFlagProvider;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

    @PostConstruct
    private void definePlsFeatureFlags() {
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            if (FeatureFlagClient.getDefinition(flag.getName()) == null) {
                FeatureFlagClient.setDefinition(flag.getName(), flag.getDefinition());
                log.info("Defined feature flag " + flag.getName());
            }
        }
    }

    @Override
    public CRMTopology getTopology(String tenantId) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            Path path = PathBuilder
                    .buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                            customerSpace.getTenantId(), customerSpace.getSpaceId())
                    .append(new Path(SPACE_CONFIGURATION_ZNODE + TOPOLOGY_ZNODE));
            return CRMTopology.fromName(camille.get(path).getData());
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18033, ex);
        }
    }

    @Override
    public String getDLRestServiceAddress(String tenantId) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            Path path = PathBuilder
                    .buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                            customerSpace.getTenantId(), customerSpace.getSpaceId())
                    .append(new Path(SPACE_CONFIGURATION_ZNODE + DL_ADDRESS_ZNODE));
            return camille.get(path).getData();
        } catch (Exception ex) {
            log.error("Can not get tenant's data loader address from ZK", ex);
            return defaultDataLoaderUrl;
        }
    }

    @Override
    public String removeDLRestServicePart(String dlRestServiceUrl) {
        String newUrl = dlRestServiceUrl;
        if (newUrl != null && newUrl.length() > 0) {
            int index = newUrl.toLowerCase().indexOf("/dlrestservice");
            if (index > -1) {
                newUrl = newUrl.substring(0, index);
            } else if (newUrl.charAt(newUrl.length() - 1) == '/') {
                newUrl = newUrl.substring(0, newUrl.length() - 1);
            }
        }
        return newUrl;
    }

    @Override
    public TenantDocument getTenantDocument(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            return batonService.getTenant(customerSpace.getContractId(), customerSpace.getTenantId());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18034, e);
        }
    }

    @Override
    public FeatureFlagValueMap getFeatureFlags(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            FeatureFlagValueMap tenantFlags = FeatureFlagClient.getFlags(customerSpace);
            tenantFlags = combineDefaultFeatureFlags(tenantFlags);
            tenantFlags = overwriteDataloaderFlags(tenantFlags, tenantId);
            tenantFlags = overwriteDeploymentWizardFlag(tenantFlags, tenantId);
            return tenantFlags;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18049, e, new String[] { tenantId });
        }
    }

    @Override
    public List<LatticeProduct> getProducts(String tenantId) {
        try {
            SpaceConfiguration spaceConfiguration = getSpaceConfiguration(tenantId);
            return spaceConfiguration.getProducts();
        } catch (Exception e) {
            log.error("Failed to get product list of tenant " + tenantId, e);
            return new ArrayList<>();
        }
    }

    @VisibleForTesting
    SpaceConfiguration getSpaceConfiguration(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            TenantDocument tenantDocument = batonService.getTenant(customerSpace.getContractId(),
                    customerSpace.getTenantId());
            return tenantDocument.getSpaceConfig();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18084, e, new String[] { tenantId });
        }
    }

    private FeatureFlagValueMap combineDefaultFeatureFlags(FeatureFlagValueMap flags) {
        FeatureFlagValueMap toReturn = defaultFeatureFlagProvider.getDefaultFlags();
        FeatureFlagDefinitionMap flagDefinitions = FeatureFlagClient.getDefinitions();
        for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
            if (flagDefinitions.containsKey(flag.getKey())) {
                toReturn.put(flag.getKey(), flag.getValue());
            }
        }
        return toReturn;
    }

    private FeatureFlagValueMap overwriteDataloaderFlags(FeatureFlagValueMap flags, String tenantId) {
        Boolean hasDlTenant = hasDataloaderFunctionalities(tenantId);
        updateFlag(flags, PlsFeatureFlag.SYSTEM_SETUP_PAGE.getName(), hasDlTenant);
        updateFlag(flags, PlsFeatureFlag.ACTIVATE_MODEL_PAGE.getName(), hasDlTenant);
        updateFlag(flags, PlsFeatureFlag.LEAD_ENRICHMENT_PAGE.getName(), hasDlTenant);
        return new FeatureFlagValueMap(flags);
    }

    /**
     * If flag already has a value, using oldValue & newValue. Otherwise, use
     * newValue
     * 
     * @param flags
     * @param flagId
     * @param value
     */
    private static void updateFlag(FeatureFlagValueMap flags, String flagId, Boolean value) {
        if (flags.containsKey(flagId)) {
            flags.put(flagId, value && flags.get(flagId));
        } else {
            flags.put(flagId, value);
        }
    }

    private Boolean hasDataloaderFunctionalities(String tenantId) {
        try {
            getTopology(tenantId);
            return true;
        } catch (LedpException e) {
            return false;
        }
    }

    private FeatureFlagValueMap overwriteDeploymentWizardFlag(FeatureFlagValueMap flags, String tenantId) {
        String flagId = PlsFeatureFlag.DEPLOYMENT_WIZARD_PAGE.getName();
        if (flags.containsKey(flagId) && !flags.get(flagId)) {
            updateFlag(flags, TenantDeploymentConstants.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, false);
        } else {
            boolean sfdcTopology = isSfdcTopology(tenantId);
            updateFlag(flags, flagId, sfdcTopology);

            boolean redirect = false;
            if (sfdcTopology) {
                redirect = needToRedirectDeploymentWizardPage(tenantId);
            }
            updateFlag(flags, TenantDeploymentConstants.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE, redirect);
        }
        return new FeatureFlagValueMap(flags);
    }

    private boolean isSfdcTopology(String tenantId) {
        try {
            CRMTopology topology = getTopology(tenantId);
            return (topology != null && CRMTopology.SFDC.getName().equals(topology.getName()));
        } catch (Exception e) {
            return false;
        }
    }

    private boolean needToRedirectDeploymentWizardPage(String tenantId) {
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
}
