package com.latticeengines.pls.service;

public interface ModelReplaceService {

    boolean replaceModel(String sourceTenantId, String sourceModelId, String targetTenantId, String targetModelId);

    boolean replaceModel(String sourceModelId, String targetTenantId, String targetModelId);

}
