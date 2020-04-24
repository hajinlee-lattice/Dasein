package com.latticeengines.apps.lp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.entitymgr.ModelFeatureImportanceEntityMgr;
import com.latticeengines.apps.lp.service.ModelFeatureImportanceService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.domain.exposed.cdl.util.FeatureImportanceMgr;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("modelFeatureImportanceService")
public class ModelFeatureImportanceServiceImpl implements ModelFeatureImportanceService {

    private static final Logger log = LoggerFactory.getLogger(ModelFeatureImportanceServiceImpl.class);

    @Inject
    private ModelFeatureImportanceEntityMgr importanceEntityMgr;

    @Inject
    private FeatureImportanceMgr featureImportanceUtil;

    @Inject
    private ModelSummaryService modelSummaryService;

    @Override
    public void upsertFeatureImportances(String customerSpace, String modelGuid) {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelGuid);
        List<ModelFeatureImportance> importancesInDb = importanceEntityMgr.getByModelGuid(modelGuid);
        if (CollectionUtils.isNotEmpty(importancesInDb)) {
            log.info("There's feature importances already.");
            return;
        }
        List<ModelFeatureImportance> importances = featureImportanceUtil.getModelFeatureImportance(customerSpace,
                modelSummary);
        importanceEntityMgr.createFeatureImportances(importances);
    }

    @Override
    public List<ModelFeatureImportance> getFeatureImportances(String customerSpace, String modelGuid) {
        return importanceEntityMgr.getByModelGuid(modelGuid);
    }

}
