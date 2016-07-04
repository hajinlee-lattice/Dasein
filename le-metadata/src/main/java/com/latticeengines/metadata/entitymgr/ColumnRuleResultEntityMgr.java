package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;

public interface ColumnRuleResultEntityMgr extends BaseEntityMgr<ColumnRuleResult> {

    List<ColumnRuleResult> findByModelId(String modelId);

}
