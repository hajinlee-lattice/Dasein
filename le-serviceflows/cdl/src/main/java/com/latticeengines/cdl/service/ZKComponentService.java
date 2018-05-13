package com.latticeengines.cdl.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

public interface ZKComponentService {

    PeriodStrategy getRollingPeriod(CustomerSpace customerSpace);

}
