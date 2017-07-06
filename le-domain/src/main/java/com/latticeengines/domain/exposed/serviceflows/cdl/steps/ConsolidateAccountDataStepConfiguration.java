package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateAccountDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    @JsonProperty("id_field")
    @NotEmptyString
    @NotNull
    private String idField;

    @JsonProperty("match_key_map")
    Map<MatchKey, List<String>> matchKeyMap = null;

    public ConsolidateAccountDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Account;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }

    public Map<MatchKey, List<String>> getMatchKeyMap() {
        return matchKeyMap;
    }

    public void setMatchKeyMap(Map<MatchKey, List<String>> matchKeyMap) {
        this.matchKeyMap = matchKeyMap;
    }
}
