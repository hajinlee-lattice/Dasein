package com.latticeengines.baton.exposed.service;

import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

public interface BatonService {

    void createTenant(String contractId, String tenantId, String spaceId, CustomerSpaceInfo spaceInfo);

    void loadDirectory(String source, String destination);

    void bootstrap(String contractId, String tenantId, String spaceId);
}
