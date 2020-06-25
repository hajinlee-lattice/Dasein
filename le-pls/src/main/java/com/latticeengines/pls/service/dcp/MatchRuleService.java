package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.match.MatchRule;

public interface MatchRuleService {

    MatchRule updateMatchRule(MatchRule matchRule, Boolean mock);

    MatchRule createMatchRule(MatchRule matchRule, Boolean mock);

    List<MatchRule> getMatchRuleList(String sourceId, Boolean includeArchived,
                                     Boolean includeInactive, Boolean mock);

    void archiveMatchRule(String matchRuleId, Boolean mock);
}
