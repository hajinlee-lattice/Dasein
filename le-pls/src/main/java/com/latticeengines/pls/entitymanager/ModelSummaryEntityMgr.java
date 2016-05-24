package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;

public interface ModelSummaryEntityMgr extends BaseEntityMgr<ModelSummary> {

    ModelSummary getByModelId(String modelId);

    ModelSummary findValidByModelId(String modelId);

    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    ModelSummary findByApplicationId(String applicationId);

    void deleteByModelId(String modelId);

    List<ModelSummary> getAll();

    List<ModelSummary> findAllValid();

    List<ModelSummary> findAllActive();

    int getTotalCount(long lastUpdateTime, boolean considerAllStatus);

    void updateStatusByModelId(String modelId, ModelSummaryStatus status);

    void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap);

    ModelSummary retrieveByModelIdForInternalOperations(String modelId);

    void updatePredictors(List<Predictor> predictors, AttributeMap attrMap);

    List<Predictor> findAllPredictorsByModelId(String modelId);

    List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId);

}
