package com.latticeengines.pls.service;

public interface ModelCopyService {

    boolean copyModel(String sourceTenant, String targetTenant, String modelId);

    boolean copyModel(String targetTenantId, String modelId);

}
