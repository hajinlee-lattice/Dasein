package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataDomain {

    SalesMarketing("D&B for Sales & Marketing"), //
    Finance("D&B for Finance"), //
    Supply("D&B for Supply"),
    Compliance("D&B for Compliance"),
    EnterpriseMasterData("D&B for Enterprise Master Data");

    private final String displayName;

    DataDomain(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }

}
