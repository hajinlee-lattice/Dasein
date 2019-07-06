package com.latticeengines.apps.lp.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.service.ModelCopyService;
import com.latticeengines.apps.lp.service.ModelMetadataService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelService;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("modelCopyService")
public class ModelCopyServiceImpl implements ModelCopyService {

    private final ModelSummaryService modelSummaryService;
    private final ModelMetadataService modelMetadataService;
    private final MetadataProxy metadataProxy;

    @Inject
    public ModelCopyServiceImpl(ModelSummaryService modelSummaryService, ModelMetadataService modelMetadataService,
            MetadataProxy metadataProxy) {
        this.modelSummaryService = modelSummaryService;
        this.modelMetadataService = modelMetadataService;
        this.metadataProxy = metadataProxy;
    }

    @Override
    public String copyModel(String sourceTenantId, String targetTenantId, String modelId) {
        sourceTenantId = CustomerSpace.parse(sourceTenantId).toString();
        targetTenantId = CustomerSpace.parse(targetTenantId).toString();

        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        String modelTypeStr = modelSummary.getModelType();
        ModelService modelService = ModelServiceBase.getModelService(modelTypeStr);

        String result;
        try (PerformanceTimer timer = new PerformanceTimer("Copy model function")) {
            result = modelService.copyModel(modelSummary, sourceTenantId, targetTenantId);
        }
        modelSummaryService.downloadModelSummary(targetTenantId, null);
        return result;
    }

    @Override
    public String copyModel(String targetTenantId, String modelId) {
        return copyModel(MultiTenantContext.getShortTenantId(), targetTenantId, modelId);
    }

    @Override
    public Table cloneTrainingTable(String modelSummaryId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Table trainingTable = modelMetadataService.getTrainingTableFromModelId(modelSummaryId);
        return metadataProxy.cloneTable(customerSpace, trainingTable.getName(), false);
    }
}
