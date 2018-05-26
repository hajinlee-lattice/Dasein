package com.latticeengines.apps.lp.service;

import com.latticeengines.domain.exposed.serviceapps.lp.ReplaceModelRequest;

public interface ModelReplaceService {

    boolean replaceModel(ReplaceModelRequest replaceModelRequest);

    boolean replaceModel(String sourceTenantId, String sourceModelId, String targetTenantId, String targetModelId);

}
