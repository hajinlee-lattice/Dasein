package com.latticeengines.baton.exposed.service;

public interface BatonService {

    void createTenant(String contractId, String tenantId, String spaceId);

    void loadDirectory(String source, String destination);
    
    void bootstrap(String contractId, String tenantId, String spaceId);
}
