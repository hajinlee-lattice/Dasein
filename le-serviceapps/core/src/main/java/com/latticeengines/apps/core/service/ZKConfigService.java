package com.latticeengines.apps.core.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ZKConfigService {

    String getFakeCurrentDate(CustomerSpace customerSpace, String componentName);

    int getInvokeTime(CustomerSpace customerSpace, String componentName);

    boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace);

    ApsRollingPeriod getRollingPeriod(CustomerSpace customerSpace, String componentName);

    int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense);

    Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, BusinessEntity businessEntity);

    Long getActiveRatingEngineQuota(CustomerSpace customerSpace, String componentName);

    Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, ProductType type);

    String getCampaignLaunchEndPointUrl(CustomerSpace customerSpace, String componentName);

    boolean isRollupDisabled(CustomerSpace customerSpace, String componentName);

    String getStack(CustomerSpace customerSpace);

    S3ImportMessageType getTriggerName(CustomerSpace customerSpace);

    Integer getLookupIdLimit(CustomerSpace customerSpace);
}
