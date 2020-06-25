package com.latticeengines.apps.dcp.repository.writer;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.repository.MatchRuleRepository;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public interface MatchRuleWriterRepository extends MatchRuleRepository {

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE MatchRuleRecord m SET m.displayName = ?2 WHERE m.matchRuleId = ?1")
    void updateMatchRule(String matchRuleId, String displayName);

    @Transactional(transactionManager = "jpaTransactionManager")
    @Modifying(clearAutomatically = true)
    @Query("UPDATE MatchRuleRecord m SET m.state = ?2 WHERE m.matchRuleId = ?1")
    void updateMatchRule(String matchRuleId, MatchRuleRecord.State state);
}
