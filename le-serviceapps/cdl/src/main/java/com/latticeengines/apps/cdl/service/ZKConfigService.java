package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;

public interface ZKConfigService {

    String getFakeCurrentDate(CustomerSpace customerSpace);

    int getInvokeTime(CustomerSpace customerSpace);

    boolean isInternalEnrichmentEnabled(CustomerSpace customerSpace);

    ApsRollingPeriod getRollingPeriod(CustomerSpace customerSpace);

}
