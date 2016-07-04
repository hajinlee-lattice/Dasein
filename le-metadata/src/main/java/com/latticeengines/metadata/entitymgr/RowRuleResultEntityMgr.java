package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;

public interface RowRuleResultEntityMgr extends BaseEntityMgr<RowRuleResult> {

    List<RowRuleResult> findByModelId(String modelId);
}
