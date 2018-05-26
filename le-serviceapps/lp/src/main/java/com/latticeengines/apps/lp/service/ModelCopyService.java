package com.latticeengines.apps.lp.service;

import com.latticeengines.domain.exposed.metadata.Table;

public interface ModelCopyService {

    String copyModel(String sourceTenant, String targetTenant, String modelId);

    String copyModel(String targetTenantId, String modelId);

    Table cloneTrainingTable(String modelSummaryId);

}
