package com.latticeengines.domain.exposed.datacloud.match;

import java.util.EnumSet;

public enum EntityMatchResult {
    UNKNOWN, MATCHED_BY_ACCOUNTID, MATCHED_BY_MATCHKEY, ORPHANED_UNMATCHED_ACCOUNTID, ORPHANED_NO_MATCH;

    private static final EnumSet<EntityMatchResult> MATCHED_RESULT = EnumSet.of(MATCHED_BY_ACCOUNTID,
            MATCHED_BY_MATCHKEY);

    private static final EnumSet<EntityMatchResult> ORPHAN_RESULT = EnumSet.of(ORPHANED_UNMATCHED_ACCOUNTID,
            ORPHANED_NO_MATCH);

    public Boolean isMatched() {
        return MATCHED_RESULT.contains(this);
    }

    public Boolean isOprhaned() {
        return ORPHAN_RESULT.contains(this);
    }
}
