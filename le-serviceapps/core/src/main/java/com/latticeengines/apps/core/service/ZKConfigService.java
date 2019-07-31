package com.latticeengines.apps.core.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ZKConfigService {

    String getFakeCurrentDate(CustomerSpace customerSpace, String componentName);

    int getInvokeTime(CustomerSpace customerSpace, String componentName);

    boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace);

    ApsRollingPeriod getRollingPeriod(CustomerSpace customerSpace, String componentName);

    int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense);

    Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, BusinessEntity businessEntity);

    Long getDataQuotaLimit(CustomerSpace customerSpace, String componentName, ProductType type);

}
