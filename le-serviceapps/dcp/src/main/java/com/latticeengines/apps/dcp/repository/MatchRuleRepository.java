package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleRepository extends BaseJpaRepository<MatchRuleRecord, Long> {

    @Query("SELECT * FROM MatchRuleRecord WHERE matchRuleId = ?1 ORDER BY versionId DESC LIMIT 1")
    MatchRuleRecord findByMatchRuleId(String matchRuleId);

    List<MatchRuleRecord> findBySourceIdAndState(String sourceId, MatchRuleRecord.State state);
}
