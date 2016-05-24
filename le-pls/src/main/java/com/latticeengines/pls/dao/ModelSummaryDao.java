package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryDao extends BaseDao<ModelSummary> {

    ModelSummary findByApplicationId(String applicationId);

    ModelSummary findByModelId(String modelId);

    ModelSummary findByModelName(String modelName);

    List<ModelSummary> findAllValid();

    List<ModelSummary> findAllActive();

    int getTotalCount(long lastUpdateTime, boolean considerAllStatus);

    ModelSummary findValidByModelId(String modelId);

}
