package com.latticeengines.apps.core.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("zKConfigService")
public class ZKConfigServiceImpl implements ZKConfigService {

    private static final Logger log = LoggerFactory.getLogger(ZKConfigServiceImpl.class);
    private static final String ACTIVE_MODEL_QUOTA = "ActiveModelQuotaLimit";
    private static final String CAMPAIGN_LAUNCH_END_POINT_URL = "CampaignLaunchEndPointUrl";
    private static final String DCP_DISABLE_ROLLUP = "DisableRollup";

    private static final String PLS = "PLS";
    private static final String STACK = "Stack";
    private static final String TriggerName = "TriggerName";

    private static final Integer DEFAULT_LOOKUP_ID_LIMIT = 80;

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
            log.warn("Failed to tell if InternalEnrichment is enabled in ZK for " + customerSpace.getTenantId() + ": "
                    + e.getMessage());
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

    @Override
    public String getCampaignLaunchEndPointUrl(CustomerSpace customerSpace, String componentName) {
        try {
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path CampaignLaunchEndPointUrlPath = path.append(CAMPAIGN_LAUNCH_END_POINT_URL);
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(CampaignLaunchEndPointUrlPath)) {
                return camille.get(CampaignLaunchEndPointUrlPath).getData();
            }
        } catch (Exception e) {
            log.warn("Failed to get count of CampaignLaunchEndPointUrl from ZK for " + customerSpace.getTenantId(), e);
        }
        return null;
    }

    @Override
    public Long getActiveRatingEngineQuota(CustomerSpace customerSpace, String componentName) {
        Long dataQuotaLimit = null;
        try {
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path activeModelCntPath = path.append(ACTIVE_MODEL_QUOTA);
            Camille camille = CamilleEnvironment.getCamille();
            // if zookeeper node value <= 0 or empty then we take the default quota limit
            // value = 50
            if (activeModelCntPath != null && camille.exists(activeModelCntPath)) {
                String activeModelsQuota = camille.get(activeModelCntPath).getData();
                if (!StringUtils.isEmpty(activeModelsQuota)) {
                    dataQuotaLimit = Long.valueOf(camille.get(activeModelCntPath).getData());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get count of ActiveModels from ZK for " + customerSpace.getTenantId(), e);
        }
        return dataQuotaLimit;
    }

    @Override
    @VisibleForTesting
    public int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense) {
        return batonService.getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, dataLicense);
    }

    @Override
    public Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, BusinessEntity businessEntity) {
        try {
            Long dataQuotaLimit = null;
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path entityDataQuotaPath = null;
            switch (businessEntity) {
                case Account:
                    entityDataQuotaPath = path.append("AccountQuotaLimit");
                    break;
                case Contact:
                    entityDataQuotaPath = path.append("ContactQuotaLimit");
                    break;
                case Product:
                    entityDataQuotaPath = path.append("ProductBundlesQuotaLimit");
                    break;
                case Transaction:
                    entityDataQuotaPath = path.append("TransactionQuotaLimit");
                    break;
                default:
                    break;
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

    @Override
    public Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, ProductType type) {
        try {
            Long dataQuotaLimit = null;
            Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName);
            Path entityDataQuotaPath = null;
            switch (type) {
                case Analytic:
                    entityDataQuotaPath = path.append("ProductBundlesQuotaLimit");
                    break;
                case Spending:
                    entityDataQuotaPath = path.append("ProductSKUsQuotaLimit");
                    break;
                default:
                    break;
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

    @Override
    public boolean isRollupDisabled(CustomerSpace customerSpace, String componentName) {
        boolean rollupReport = false;
        try {
            Path rollupPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    componentName).append(DCP_DISABLE_ROLLUP);
            Camille camille = CamilleEnvironment.getCamille();
            if (rollupPath != null && camille.exists(rollupPath)) {
                rollupReport = Boolean.parseBoolean(camille.get(rollupPath).getData());
            }
        } catch (Exception e) {
            log.info("failed to get rollup flag from zk {} ", customerSpace);
        }
        return rollupReport;
    }

    @Override
    public String getStack(CustomerSpace customerSpace) {
        String stack = "";
        try {
            stack = getValueFromZK(customerSpace, PathConstants.CDL, STACK);
        } catch (Exception e) {
            log.warn("Failed to get stack name from ZK for " + customerSpace.getTenantId(), e);
        }
        return stack;
    }

    @Override
    public S3ImportMessageType getTriggerName(CustomerSpace customerSpace) {
        S3ImportMessageType messageType = S3ImportMessageType.UNDEFINED;
        try {
            String value = getValueFromZK(customerSpace, PathConstants.CDL, TriggerName);
            if (StringUtils.isNotEmpty(value)) {
                messageType = S3ImportMessageType.fromName(value);
            }
        } catch (Exception e) {
            log.warn("Failed to get trigger name from ZK for " + customerSpace.getTenantId(), e);
        }
        return messageType;
    }

    @Override
    public Integer getLookupIdLimit(CustomerSpace customerSpace) {
        // No tenant level config right now. Only check env level
        Path path = PathBuilder.buildLookupIdLimitEnvConfigPath(CamilleEnvironment.getPodId());
        Camille camille = CamilleEnvironment.getCamille();
        try {
            if (camille.exists(path)) {
                String rawValue = camille.get(path).getData();
                return Integer.parseInt(rawValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DEFAULT_LOOKUP_ID_LIMIT;
    }

    private String getValueFromZK(CustomerSpace customerSpace, String componentName, String pathToAppend) throws Exception {
        String value = null;
        Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace, componentName).append(pathToAppend);
        Camille camille = CamilleEnvironment.getCamille();
        if (camille.exists(path)) {
            value = camille.get(path).getData();
        }
        return value;
    }

}
