package com.latticeengines.scoringapi.exception;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringApiException extends LedpException {

    private static final long serialVersionUID = -2770149136157809201L;

    public ScoringApiException(LedpCode code) {
        super(code);
    }

    public ScoringApiException(LedpCode code, String[] params) {
        super(code, params);
    }

}
