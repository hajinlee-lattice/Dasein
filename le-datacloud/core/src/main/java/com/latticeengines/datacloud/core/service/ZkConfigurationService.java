package com.latticeengines.datacloud.core.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface ZkConfigurationService {

    boolean isMatchDebugEnabled(CustomerSpace customerSpace);

    boolean useRemoteDnBGlobal();

    boolean isCDLTenant(CustomerSpace customerSpace);

    boolean isPublicDomainCheckRelaxed();

}
