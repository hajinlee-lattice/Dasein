package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface ZKConfigService {

    String getFakeCurrentDate(CustomerSpace customerSpace);

    int getInvokeTime(CustomerSpace customerSpace);

}
