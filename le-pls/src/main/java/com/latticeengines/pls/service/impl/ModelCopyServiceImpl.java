package com.latticeengines.pls.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelService;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelCopyService")
public class ModelCopyServiceImpl implements ModelCopyService {

    @SuppressWarnings("unused")
    private static Logger log = LoggerFactory.getLogger(ModelCopyServiceImpl.class);

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Override
    public boolean copyModel(String sourceTenantId, String targetTenantId, String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        String modelTypeStr = modelSummary != null ? modelSummary.getModelType() : ModelType.PYTHONMODEL.getModelType();
        ModelService modelService = ModelServiceBase.getModelService(modelTypeStr);

        try (PerformanceTimer timer = new PerformanceTimer("Copy model function")) {
            return modelService.copyModel(modelSummary, sourceTenantId, targetTenantId);
        }
    }

    @Override
    public boolean copyModel(String targetTenantId, String modelId) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        return copyModel(space.toString(), targetTenantId, modelId);
    }
}
