package com.latticeengines.apps.lp.entitymgr;

import java.util.List;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryEntityMgr extends BaseEntityMgr<ModelSummary> {

    ModelSummary getByModelId(String modelId);

    ModelSummary findValidByModelId(String modelId);

    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    ModelSummary findByApplicationId(String applicationId);

    List<ModelSummary> getModelSummariesByApplicationId(String applicationId);

    ModelSummary getByModelNameInTenant(String modelName, Tenant tenant);

    void deleteByModelId(String modelId);

    List<ModelSummary> getAll();

    List<String> getAllModelSummaryIds();

    List<ModelSummary> getAllByTenant(Tenant tenant);

    List<ModelSummary> findAllValid();

    List<ModelSummary> findAllActive();

    int findTotalCount(long lastUpdateTime, boolean considerAllStatus);

    List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset, int maximum);

    void updateStatusByModelId(String modelId, ModelSummaryStatus status);

    void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap);

    ModelSummary retrieveByModelIdForInternalOperations(String modelId);

    void deleteByModelIdForInternalOperations(String modelId);

    void updatePredictors(List<Predictor> predictors, AttributeMap attrMap);

    List<Predictor> findAllPredictorsByModelId(String modelId);

    List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId);

    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame);

    void updateLastUpdateTime(ModelSummary summary);

    void updateLastUpdateTime(String modelGuid);

    boolean hasBucketMetadata(String modelId);

    List<ModelSummary> findModelSummariesByIds(Set<String> ids);
}
