package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleRepository extends BaseJpaRepository<MatchRuleRecord, Long> {

    MatchRuleRecord findTopByMatchRuleIdAndStateOrderByVersionIdDesc(String matchRuleId, MatchRuleRecord.State state);

    MatchRuleRecord findTopBySourceIdAndRuleTypeAndStateOrderByVersionIdDesc(String sourceId,
                                                                             MatchRuleRecord.RuleType ruleType,
                                                                             MatchRuleRecord.State state);

    List<MatchRuleRecord> findBySourceIdAndState(String sourceId, MatchRuleRecord.State state);

    @Query("SELECT m FROM MatchRuleRecord m WHERE m.sourceId = :sourceId AND m.state IN (:states)")
    List<MatchRuleRecord> findBySourceIdAndStates(@Param("sourceId")String sourceId,
                                                 @Param("states")List<MatchRuleRecord.State> states);

    boolean existsByMatchRuleId(String matchRuleId);

    boolean existsBySourceIdAndRuleTypeAndState(String sourceId, MatchRuleRecord.RuleType ruleType, MatchRuleRecord.State state);

}
