package com.latticeengines.domain.exposed.datacloud.match;

public enum OperationalMode {
    LDC_MATCH, //
    MULTI_CANDIDATES, // Multiple candidates -> could be converted to a boolean flag
    // CDL match before M25 (Account attributes lookup by AccountId)
    CDL_LOOKUP, //
    ENTITY_MATCH(true), //
    ENTITY_MATCH_ATTR_LOOKUP(true); //

    public final boolean isEntityMatch;

    OperationalMode() {
        isEntityMatch = false;
    }

    OperationalMode(boolean isEntityMatch) {
        this.isEntityMatch = isEntityMatch;
    }

    /*
     * null-safe helper for entity match mode check
     */
    public static boolean isEntityMatch(OperationalMode mode) {
        return mode != null && mode.isEntityMatch;
    }
}
