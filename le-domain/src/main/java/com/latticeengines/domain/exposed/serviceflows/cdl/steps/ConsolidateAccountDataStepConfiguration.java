package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateAccountDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    @JsonProperty("match_key_map")
    Map<MatchKey, List<String>> matchKeyMap = null;

    public ConsolidateAccountDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Account;
    }

    public Map<MatchKey, List<String>> getMatchKeyMap() {
        return matchKeyMap;
    }

    public void setMatchKeyMap(Map<MatchKey, List<String>> matchKeyMap) {
        this.matchKeyMap = matchKeyMap;
    }
}
