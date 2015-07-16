package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryService {

    ModelSummary createModelSummary(String rawModelSummary, String tenantId);

    ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId);

    boolean modelIdinTenant(String modelId, String tenantId);

}
