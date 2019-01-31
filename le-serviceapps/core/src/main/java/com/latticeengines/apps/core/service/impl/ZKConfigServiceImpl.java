package com.latticeengines.apps.core.service.impl;

import javax.inject.Inject;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("zKConfigService")
public class ZKConfigServiceImpl implements ZKConfigService {

    private static final Logger log = LoggerFactory.getLogger(ZKConfigServiceImpl.class);
    private static final String DATA_CLOUD_LICENSE = "/DataCloudLicense";
    private static final String MAX_ENRICH_ATTRIBUTES = "/MaxEnrichAttributes";
    private static final String PLS = "PLS";

    @Inject
    private BatonService batonService;

    @Override
    public String getFakeCurrentDate(CustomerSpace customerSpace, String componentName) {
        try {
            String fakeCurrentDate = null;
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(fakeCurrentDatePath)) {
                fakeCurrentDate = camille.get(fakeCurrentDatePath).getData();
            }
            return fakeCurrentDate;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get FakeCurrentDate from ZK for " + customerSpace.getTenantId(), e);
        }
    }

    @Override
    public int getInvokeTime(CustomerSpace customerSpace, String componentName) {
        try {
            int invokeTime = 0;
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path invokeTimePath = cdlPath.append("InvokeTime");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(invokeTimePath)) {
                invokeTime = Integer.parseInt(camille.get(invokeTimePath).getData());
            }
            return invokeTime;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get InvokeTime from ZK for " + customerSpace.getTenantId(), e);
        }
    }

    @Override
    public boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace) {
        try {
            return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
        } catch (Exception e) {
            log.warn("Failed to tell if InternalEnrichment is enabled in ZK for " + customerSpace.getTenantId() + ": " + e.getMessage());
            return false;
        }
    }

    @Override
    public ApsRollingPeriod getRollingPeriod(CustomerSpace customerSpace, String componentName) {
        ApsRollingPeriod period = ApsRollingPeriod.BUSINESS_MONTH;
        try {
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path dataPath = cdlPath.append("DefaultAPSRollupPeriod");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(dataPath)) {
                String data = camille.get(dataPath).getData();
                period = ApsRollingPeriod.fromName(data);
            }
        } catch (Exception e) {
            log.warn("Failed to get DefaultAPSRollupPeriod from ZK for " + customerSpace.getTenantId(), e);
        }
        return period;
    }

    @VisibleForTesting
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
            Path defaultConfigPath = null;
            if (dataLicense == null) {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path(MAX_ENRICH_ATTRIBUTES)));
            } else {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path("/" + dataLicense)));
            }

            try {
                maxPremiumLeadEnrichmentAttributes = camille.get(defaultConfigPath).getData();
            } catch (Exception e) {
                throw new RuntimeException("Cannot get default value for maximum premium lead enrichment attributes ");
            }
            try {
                Integer attrNumber = Integer.parseInt(maxPremiumLeadEnrichmentAttributes);
                camille.upsert(path, DocumentUtils.toRawDocument(attrNumber), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException("Cannot update value for maximum premium lead enrichment attributes ");
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot get maximum premium lead enrichment attributes ", e);
        }
        return Integer.parseInt(maxPremiumLeadEnrichmentAttributes);
    }

    @Override
    public Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, BusinessEntity businessEntity) {
        try {
            Long dataQuotaLimit = null;
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path entityDataQuotaPath = null;
            switch (businessEntity) {
                case Account: entityDataQuotaPath = path.append("AccountQuotaLimit");break;
                case Contact: entityDataQuotaPath = path.append("ContactQuotaLimit");break;
                case Product: entityDataQuotaPath = path.append("ProductBundlesQuotaLimit");break;
                case Transaction: entityDataQuotaPath = path.append("TransactionQuotaLimit");break;
                default:break;
            }
            Camille camille = CamilleEnvironment.getCamille();
            if (entityDataQuotaPath != null && camille.exists(entityDataQuotaPath)) {
                dataQuotaLimit = Long.valueOf(camille.get(entityDataQuotaPath).getData());
            }
            return dataQuotaLimit;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get DataQuotaLimit from ZK for " + customerSpace.getTenantId(), e);
        }
    }
}
