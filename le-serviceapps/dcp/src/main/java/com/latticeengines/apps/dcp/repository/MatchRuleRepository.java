package com.latticeengines.apps.dcp.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleRepository extends BaseJpaRepository<MatchRuleRecord, Long> {

    MatchRuleRecord findTopByMatchRuleIdAndStateOrderByVersionIdDesc(String matchRuleId, MatchRuleRecord.State state);

    MatchRuleRecord findTopBySourceIdAndRuleTypeAndStateOrderByVersionIdDesc(String sourceId,
                                                                             MatchRuleRecord.RuleType ruleType,
                                                                             MatchRuleRecord.State state);

    List<MatchRuleRecord> findBySourceIdAndState(String sourceId, MatchRuleRecord.State state);

    boolean existsByMatchRuleId(String matchRuleId);

    boolean existsBySourceIdAndRuleTypeAndState(String sourceId, MatchRuleRecord.RuleType ruleType, MatchRuleRecord.State state);

}
