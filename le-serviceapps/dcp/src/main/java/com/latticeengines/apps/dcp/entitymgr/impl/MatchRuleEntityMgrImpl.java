package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.MatchRuleDao;
import com.latticeengines.apps.dcp.entitymgr.MatchRuleEntityMgr;
import com.latticeengines.apps.dcp.repository.MatchRuleRepository;
import com.latticeengines.apps.dcp.repository.reader.MatchRuleReaderRepository;
import com.latticeengines.apps.dcp.repository.writer.MatchRuleWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

@Component("matchRuleEntityMgr")
public class MatchRuleEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<MatchRuleRepository, MatchRuleRecord, Long>
        implements MatchRuleEntityMgr {

    @Inject
    private MatchRuleEntityMgrImpl _self;

    @Inject
    private MatchRuleDao matchRuleDao;

    @Inject
    private MatchRuleReaderRepository matchRuleReaderRepository;

    @Inject
    private MatchRuleWriterRepository matchRuleWriterRepository;

    @Override
    protected MatchRuleRepository getReaderRepo() {
        return matchRuleReaderRepository;
    }

    @Override
    protected MatchRuleRepository getWriterRepo() {
        return matchRuleWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<MatchRuleRepository, MatchRuleRecord, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<MatchRuleRecord> getDao() {
        return matchRuleDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MatchRuleRecord findTopActiveMatchRule(String matchRuleId) {
        return getReadOrWriteRepository().findTopByMatchRuleIdAndStateOrderByVersionIdDesc(matchRuleId, MatchRuleRecord.State.ACTIVE);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MatchRuleRecord> findMatchRules(String sourceId, MatchRuleRecord.State state) {
        return getReadOrWriteRepository().findBySourceIdAndState(sourceId, state);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MatchRuleRecord> findMatchRules(String sourceId, List<MatchRuleRecord.State> states) {
        return getReadOrWriteRepository().findBySourceIdAndStates(sourceId, states);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean existMatchRule(String matchRuleId) {
        return getReadOrWriteRepository().existsByMatchRuleId(matchRuleId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean existMatchRule(String sourceId, MatchRuleRecord.RuleType ruleType) {
        return getReadOrWriteRepository().existsBySourceIdAndRuleTypeAndState(sourceId, ruleType, MatchRuleRecord.State.ACTIVE);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateMatchRule(String matchRuleId, String displayName) {
        matchRuleWriterRepository.updateMatchRule(matchRuleId, displayName);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateMatchRule(String matchRuleId, MatchRuleRecord.State state) {
        matchRuleWriterRepository.updateMatchRule(matchRuleId, state);
    }

}
