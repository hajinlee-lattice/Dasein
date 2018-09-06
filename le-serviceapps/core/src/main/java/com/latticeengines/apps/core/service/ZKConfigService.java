package com.latticeengines.apps.core.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;

public interface ZKConfigService {

    String getFakeCurrentDate(CustomerSpace customerSpace, String componentName);

    int getInvokeTime(CustomerSpace customerSpace, String componentName);

    boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace);

    ApsRollingPeriod getRollingPeriod(CustomerSpace customerSpace, String componentName);

    int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense);

}
