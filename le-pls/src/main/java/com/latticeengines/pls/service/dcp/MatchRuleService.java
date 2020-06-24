package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.match.MatchRule;

public interface MatchRuleService {

    MatchRule updateMatchRule(MatchRule matchRule);

    MatchRule createMatchRule(MatchRule matchRule);

    List<MatchRule> getMatchRuleList(String sourceId, Boolean includeArchived,
                                     Boolean includeInactive);

    void archiveMatchRule(String matchRuleId);
}
