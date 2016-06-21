package com.latticeengines.pls.service;

public interface ModelCopyService {

    Boolean copyModel(String sourceTenant, String targetTenant, String modelId);

    Boolean copyModel(String targetTenantId, String modelId);

}
