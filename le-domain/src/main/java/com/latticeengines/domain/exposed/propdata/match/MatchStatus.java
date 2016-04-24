package com.latticeengines.domain.exposed.propdata.match;

import java.util.EnumSet;

public enum MatchStatus {
    FAILED,
    NEW,
    MATCHING,
    MATCHED,
    FINISHING,
    ABORTED,
    FINISHED;

    private static final EnumSet<MatchStatus> TERMINAL_STATUS = EnumSet.of(MatchStatus.FINISHED,
            MatchStatus.ABORTED, MatchStatus.FAILED);

    public Boolean isTerminal() {
        return  TERMINAL_STATUS.contains(this);
    }
}
