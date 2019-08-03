package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.app.exposed.util.ValidateEnrichAttributesUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("appTenantConfigService")
public class CommonTenantConfigServiceImpl implements CommonTenantConfigService {
    private static final String ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE = "/EnrichAttributesMaxNumber";
    private static final String DATA_CLOUD_LICENSE = "/DataCloudLicense";
    private static final String MAX_ENRICH_ATTRIBUTES = "/MaxEnrichAttributes";
    private static final Logger log = LoggerFactory.getLogger(CommonTenantConfigServiceImpl.class);
    public static final String PLS = "PLS";

    @Inject
    private BatonService batonService;

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
            return batonService.getFeatureFlags(customerSpace);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18049, e, new String[] { tenantId });
        }
    }

    @Override
    public int getMaxPremiumLeadEnrichmentAttributes(String tenantId) {
        String maxPremiumLeadEnrichmentAttributes;
        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath = null;
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            contractPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace, PLS);
            Path path = contractPath.append(ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE);
            maxPremiumLeadEnrichmentAttributes = camille.get(path).getData();
        } catch (KeeperException.NoNodeException ex) {
            log.error(
                    "Will replace maxPremiumLeadEnrichmentAttributes with the default value since there is none for the tenant: "
                            + tenantId);
            Path defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                    .append(new Path(ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE));
            String defaultPremiumLeadEnrichmentAttributes;
            try {
                defaultPremiumLeadEnrichmentAttributes = camille.get(defaultConfigPath).getData();
            } catch (Exception e) {
                throw new RuntimeException("Cannot get default value for maximum premium lead enrichment attributes ");
            }
            try {
                Integer attrNumber = Integer.parseInt(defaultPremiumLeadEnrichmentAttributes);
                camille.upsert(contractPath, DocumentUtils.toRawDocument(attrNumber),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Illegal data or cannot update value for maximum premium lead enrichment attributes ");
            }
            return ValidateEnrichAttributesUtils.validateEnrichAttributes(defaultPremiumLeadEnrichmentAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Cannot get maximum premium lead enrichment attributes ", e);
        }

        return ValidateEnrichAttributesUtils.validateEnrichAttributes(maxPremiumLeadEnrichmentAttributes);
    }

    @Override
    public int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense) {
        String maxPremiumLeadEnrichmentAttributes;
        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath = null;
        Path path = null;
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            contractPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace, PLS);
            if (dataLicense == null) {
                path = contractPath.append(DATA_CLOUD_LICENSE).append(MAX_ENRICH_ATTRIBUTES);
            } else {
                path = contractPath.append(DATA_CLOUD_LICENSE).append("/" + dataLicense);
            }
            maxPremiumLeadEnrichmentAttributes = camille.get(path).getData();
        } catch (KeeperException.NoNodeException ex) {
            log.error("Will replace maxPremiumLeadEnrichmentAttributes with the default value since there is none for the tenant: "
                    + tenantId);
            Path defaultConfigPath = null;
            if (dataLicense == null) {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path(MAX_ENRICH_ATTRIBUTES)));
            } else {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path("/" + dataLicense)));
            }

            String defaultPremiumLeadEnrichmentAttributes;
            try {
                defaultPremiumLeadEnrichmentAttributes = camille.get(defaultConfigPath).getData();
            } catch (Exception e) {
                throw new RuntimeException("Cannot get default value for maximum premium lead enrichment attributes ");
            }
            try {
                Integer attrNumber = Integer.parseInt(defaultPremiumLeadEnrichmentAttributes);
                camille.upsert(path, DocumentUtils.toRawDocument(attrNumber),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Illegal data or cannot update value for maximum premium lead enrichment attributes ");
            }
            return ValidateEnrichAttributesUtils.validateEnrichAttributes(defaultPremiumLeadEnrichmentAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Cannot get maximum premium lead enrichment attributes ", e);
        }

        return ValidateEnrichAttributesUtils.validateEnrichAttributes(maxPremiumLeadEnrichmentAttributes);
    }

    @Override
    public TenantConfiguration getTenantConfiguration() {
        Tenant tenant = MultiTenantContext.getTenant();
        FeatureFlagValueMap featureFlagValueMap = getFeatureFlags(tenant.getId());
        List<LatticeProduct> products = getProducts(tenant.getId());

        TenantConfiguration tenantConfiguration = new TenantConfiguration();
        tenantConfiguration.setFeatureFlagValueMap(featureFlagValueMap);
        tenantConfiguration.setProducts(products);

        return tenantConfiguration;
    }

    @Override
    public boolean isEntityMatchEnabled() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return batonService.isEntityMatchEnabled(customerSpace);
    }

    @Override
    public boolean onlyEntityMatchGAEnabled() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return batonService.onlyEntityMatchGAEnabled(customerSpace);
    }

    private SpaceConfiguration getSpaceConfiguration(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            TenantDocument tenantDocument = batonService.getTenant(customerSpace.getContractId(),
                    customerSpace.getTenantId());
            return tenantDocument.getSpaceConfig();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18086, e, new String[] { tenantId });
        }
    }

}
