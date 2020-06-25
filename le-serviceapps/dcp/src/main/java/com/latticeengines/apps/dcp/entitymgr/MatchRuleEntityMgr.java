package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleEntityMgr extends BaseEntityMgrRepository<MatchRuleRecord, Long> {

    MatchRuleRecord findTopActiveMatchRule(String matchRuleId);

    List<MatchRuleRecord> findMatchRules(String sourceId, MatchRuleRecord.State state);

//    MatchRuleRecord findActiveBaseMatchRule(String sourceId);

    boolean existMatchRule(String matchRuleId);

    boolean existMatchRule(String sourceId, MatchRuleRecord.RuleType ruleType);

    void updateMatchRule(String matchRuleId, String displayName);

    void updateMatchRule(String matchRuleId, MatchRuleRecord.State state);
}
