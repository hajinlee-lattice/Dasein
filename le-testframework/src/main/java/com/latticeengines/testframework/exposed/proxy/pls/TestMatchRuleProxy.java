package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;

@Component("testMatchRuleProxy")
public class TestMatchRuleProxy extends PlsRestApiProxyBase {

    public TestMatchRuleProxy() {
        super("pls/matchrules");
    }

    public List<MatchRule> getMatchRuleList(String sourceId, Boolean includeArchived, Boolean includeInactive) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        if (Boolean.TRUE.equals(includeArchived)) {
            url += "?includeArchived=true";
            if (Boolean.TRUE.equals(includeInactive)) {
                url += "&includeInactive=true";
            }
        } else {
            if (Boolean.TRUE.equals(includeInactive)) {
                url += "?includeInactive=true";
            }
        }
        List<?> rawList = get("Get match rule by sourceId", url, List.class);
        return JsonUtils.convertList(rawList, MatchRule.class);
    }

    public MatchRule updateMatchRule(MatchRule matchRule) {
        String url = constructUrl();
        return put("Update match rule", url, matchRule, MatchRule.class);
    }

    public MatchRule createMatchRule(MatchRule matchRule) {
        String url = constructUrl();
        return put("Create match rule", url, matchRule, MatchRule.class);
    }

    public void deleteMatchRule(String matchRuleId) {
        String url = constructUrl("/{matchRuleId}", matchRuleId);
        delete("Archive match rule", url);
    }
}
