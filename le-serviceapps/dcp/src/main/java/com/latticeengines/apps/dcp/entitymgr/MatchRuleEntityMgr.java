package com.latticeengines.apps.dcp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleEntityMgr extends BaseEntityMgrRepository<MatchRuleRecord, Long> {

    MatchRuleRecord findMatchRule(String matchRuleId);
}
