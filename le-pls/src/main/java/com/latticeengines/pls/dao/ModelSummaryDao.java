package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryDao extends BaseDao<ModelSummary> {

    ModelSummary findByModelId(String modelId);

    ModelSummary findByModelName(String modelName);

}
