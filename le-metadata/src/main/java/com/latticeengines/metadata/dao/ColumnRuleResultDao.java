package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;

public interface ColumnRuleResultDao extends BaseDao<ColumnRuleResult> {

    List<ColumnRuleResult> findByModelId(String modelId);
}
