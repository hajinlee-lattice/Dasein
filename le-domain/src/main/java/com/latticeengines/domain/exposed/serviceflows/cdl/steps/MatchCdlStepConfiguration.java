package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class MatchCdlStepConfiguration extends MicroserviceStepConfiguration {

    private String matchInputTableName;

    public String getMatchInputTableName() {
        return matchInputTableName;
    }

    public void setMatchInputTableName(String matchInputTableName) {
        this.matchInputTableName = matchInputTableName;
    }

}
