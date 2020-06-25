package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("matchRuleProxy")
public class MatchRuleProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected MatchRuleProxy() {
        super("dcp");
    }

    public MatchRule createMatchRule(String customerSpace, MatchRule matchRule) {
        String url = constructUrl("/customerspaces/{customerSpace}/matchrules", shortenCustomerSpace(customerSpace));
        return put("Create match rule", url, matchRule, MatchRule.class);
    }

    public MatchRule updateMatchRule(String customerSpace, MatchRule matchRule) {
        String url = constructUrl("/customerspaces/{customerSpace}/matchrules", shortenCustomerSpace(customerSpace));
        return put("Update match rule", url, matchRule, MatchRule.class);
    }

    public void deleteMatchRule(String customerSpace, String matchRuleId) {
        String url = constructUrl("/customerspaces/{customerSpace}/matchrules/{matchRuleId}", shortenCustomerSpace(customerSpace), matchRuleId);
        delete("Archive match rule", url);
    }

    public List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived,
                                            Boolean includeInactive) {
        String url = constructUrl("/customerspaces/{customerSpace}/matchrules/sourceId/{sourceId}",
                shortenCustomerSpace(customerSpace), sourceId);
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

    public MatchRuleConfiguration getMatchConfig(String customerSpace, String sourceId) {
        String url = constructUrl("/customerspaces/{customerSpace}/matchrules/sourceId/{sourceId}/matchconfig",
                shortenCustomerSpace(customerSpace), sourceId);
        return get("Get match config", url, MatchRuleConfiguration.class);
    }

}
