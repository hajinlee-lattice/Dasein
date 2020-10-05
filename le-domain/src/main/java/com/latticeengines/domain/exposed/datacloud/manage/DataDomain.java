package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataDomain {

    SalesMarketing("D&B for Sales & Marketing"), //
    Finance("D&B for Finance"), //
    Supply("D&B for Supply"),
    Compliance("D&B for Compliance"),
    EnterpriseMasterData("D&B for Enterprise Master Data");

    private final String displayName;

    private static Map<String, DataDomain> displayNameMap = new HashMap<>();
    static {
        for (DataDomain domain: DataDomain.values()) {
            displayNameMap.put(domain.getDisplayName().toLowerCase(), domain);
        }
    }

    DataDomain(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }

    public static DataDomain parse(String str) {
        if (displayNameMap.containsKey(str.toLowerCase())) {
            return displayNameMap.get(str.toLowerCase());
        } else {
            return DataDomain.valueOf(str);
        }
    }

}
