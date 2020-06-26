package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;

public interface MatchRuleService {

    MatchRule updateMatchRule(String customerSpace, MatchRule matchRule);

    MatchRule createMatchRule(String customerSpace, MatchRule matchRule);

    List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived,
                                     Boolean includeInactive);

    void archiveMatchRule(String customerSpace, String matchRuleId);

    MatchRuleConfiguration getMatchConfig(String customerSpace, String sourceId);
}
