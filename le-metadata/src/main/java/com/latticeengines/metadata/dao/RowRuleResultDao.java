package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;

public interface RowRuleResultDao extends BaseDao<RowRuleResult> {

    List<RowRuleResult> findByModelId(String modelId);
}
