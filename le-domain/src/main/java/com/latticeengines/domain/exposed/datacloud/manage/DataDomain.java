package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataDomain {

    SalesMarketing("Sales and Marketing"), //
    Finance("Finance"), //
    Supply("Supply"),
    Compliance("Compliance"),
    EnterpriseMasterData("Enterprise Master Data");

    private final String displayName;

    DataDomain(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }

}
