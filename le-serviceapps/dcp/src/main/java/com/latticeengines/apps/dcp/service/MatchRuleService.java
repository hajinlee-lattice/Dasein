package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;

public interface MatchRuleService {

    /**
     * Update existing match rule
     * 1. MatchRule cannot be null
     * 2. MatchRule.matchRuleId cannot be empty
     * 3. MatchRule.sourceId should be same as original MatchRule.
     * @param customerSpace Tenant.
     * @param matchRule New MatchRule
     * @return updated MatchRule
     */
    MatchRule updateMatchRule(String customerSpace, MatchRule matchRule);

    /**
     * Create MatchRule
     * 1. MatchRule cannot be null
     * 2. MatchRule.sourceId cannot be empty.
     * @param customerSpace Tenant.
     * @param matchRule MatchRule to be created.
     * @return created MatchRule
     */
    MatchRule createMatchRule(String customerSpace, MatchRule matchRule);

    List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived,
                                     Boolean includeInactive);

    void archiveMatchRule(String customerSpace, String matchRuleId);

    MatchRuleConfiguration getMatchConfig(String customerSpace, String sourceId);

    void hardDeleteMatchRuleBySourceId(String customerSpace, String sourceId);
}
