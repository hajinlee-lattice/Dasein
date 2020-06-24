package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.MatchRuleEntityMgr;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

@Service("matchRuleService")
public class MatchRuleServiceImpl implements MatchRuleService {

    @Inject
    private MatchRuleEntityMgr matchRuleEntityMgr;

    @Override
    public MatchRule updateMatchRule(String customerSpace, MatchRule matchRule) {
        if (matchRule == null) {
            throw new IllegalArgumentException("Cannot update NULL match rule!");
        }
        MatchRuleRecord matchRuleRecord = matchRuleEntityMgr.findMatchRule(matchRule.getMatchRuleId());
        if (matchRuleRecord == null) {
            throw new IllegalArgumentException("Cannot find match rule with id: " + matchRule.getMatchRuleId());
        }

        return null;
    }

    @Override
    public MatchRule createMatchRule(String customerSpace, MatchRule matchRule) {
        return null;
    }

    @Override
    public List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived, Boolean includeInactive) {
        return null;
    }

    @Override
    public void archiveMatchRule(String customerSpace, String matchRuleId) {

    }
}
