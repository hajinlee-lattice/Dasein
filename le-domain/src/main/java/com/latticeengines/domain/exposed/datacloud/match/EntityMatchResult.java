package com.latticeengines.domain.exposed.datacloud.match;

import java.util.EnumSet;

// Enumeration for describing the result of an Entity Match (currently handles Lead-to-Account).
// Cases:
//   MATCHED_BY_ACCOUNTID: Contact's account was found by matching customer provided account ID to existing account.
//   MATCHED_BY_MATCHKEY: Contact's account was found by matching existing account using match keys.
//   ORPHANED_UNMATCHED_ACCOUNTID: Customer provided account ID for contact did not match account ID found through
//     match.
//   ORPHANED_NO_MATCH: Could not match contact either by using customer provided account ID or match keys.
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
