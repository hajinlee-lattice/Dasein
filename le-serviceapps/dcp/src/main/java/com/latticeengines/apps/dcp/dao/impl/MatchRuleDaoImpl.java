package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.MatchRuleDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

@Component("matchRuleDao")
public class MatchRuleDaoImpl extends BaseDaoImpl<MatchRuleRecord> implements MatchRuleDao {
    @Override
    protected Class<MatchRuleRecord> getEntityClass() {
        return MatchRuleRecord.class;
    }
}
