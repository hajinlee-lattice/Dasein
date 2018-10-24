package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Patch result of one PatchBook entry
 */
public enum PatchStatus {
    Patched("Patched"), Failed("Failed"), Inactive("Inactive"), NewInactive("NewInactive"), Noop("No-op");

    private String displayName;

    PatchStatus(String displayName) {
        this.displayName = displayName;
    }

    // serialized to displayName instead of the literal
    @JsonValue
    public String getDisplayName() {
        return displayName;
    }
}
