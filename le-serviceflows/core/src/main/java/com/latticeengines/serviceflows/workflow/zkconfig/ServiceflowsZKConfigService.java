package com.latticeengines.serviceflows.workflow.zkconfig;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface ServiceflowsZKConfigService {

    boolean isEnabledForInternalEnrichment(CustomerSpace customerSpace);

}
