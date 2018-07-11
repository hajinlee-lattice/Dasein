package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryDao extends BaseDao<ModelSummary> {

    ModelSummary findByApplicationId(String applicationId);

    ModelSummary findByModelId(String modelId);

    ModelSummary findByModelName(String modelName);

    List<ModelSummary> getAllByTenant(Tenant tenant);

    List<String> getAllModelSummaryIds();

    List<ModelSummary> findAllValid();

    List<ModelSummary> findAllActive();

    int findTotalCount(long lastUpdateTime, boolean considerAllStatus, boolean considerDeleted);

    List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset, int maximum,
            boolean considerDeleted);

    ModelSummary findValidByModelId(String modelId);

    ModelSummary getByModelNameInTenant(String modelName, Tenant tenant);

    List<ModelSummary> getModelSummariesByApplicationId(String applicationId);

    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame);

    boolean hasBucketMetadata(String modelId);

}
