package com.latticeengines.apps.lp.service;

public interface ModelCopyService {

    String copyModel(String sourceTenant, String targetTenant, String modelId);

    String copyModel(String targetTenantId, String modelId);

}
