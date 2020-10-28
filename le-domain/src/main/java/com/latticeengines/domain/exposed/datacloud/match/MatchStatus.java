package com.latticeengines.domain.exposed.datacloud.match;

import java.util.EnumSet;

public enum MatchStatus {
    FAILED, NEW, MATCHING, MATCHED, FINISHING, ABORTED, PARTIAL_SUCCESS, FINISHED;

    private static final EnumSet<MatchStatus> TERMINAL_STATUS = EnumSet.of(MatchStatus.FINISHED,
            MatchStatus.PARTIAL_SUCCESS, MatchStatus.ABORTED, MatchStatus.FAILED);

    public Boolean isTerminal() {
        return TERMINAL_STATUS.contains(this);
    }
}
