package com.latticeengines.domain.exposed.datacloud.contactmaster;

import com.fasterxml.jackson.annotation.JsonValue;

public enum LiveRampDestination {
    Google("Google DV360"), //
    Tradedesk("The Trade Desk"), //
    Mediamath("MediaMath"), //
    Verizon("Verizon OATH"), //
    AppNexus("AppNexus"), //
    Adobe("Adobe Audience Manager");

    private final String displayName;

    LiveRampDestination(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }
}
