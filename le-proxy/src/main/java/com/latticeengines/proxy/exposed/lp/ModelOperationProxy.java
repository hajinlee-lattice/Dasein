package com.latticeengines.proxy.exposed.lp;

public interface ModelOperationProxy {

    Boolean cleanUpModel(String modelId);

    Boolean replaceModel(String sourceTenantId, String sourceModelId, String targetTenantId, String targetModelId);

}
