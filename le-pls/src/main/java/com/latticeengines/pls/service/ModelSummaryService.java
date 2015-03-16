package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryService {

    void resolveNameIdConflict(ModelSummary modelSummary, String tenantId);

}
