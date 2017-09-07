package com.latticeengines.scoringapi.exposed.exception;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringApiException extends LedpException {

    private static final long serialVersionUID = -2770149136157809201L;

    private String detailedMessage;

    public ScoringApiException(LedpCode code) {
        super(code);
    }

    public ScoringApiException(LedpCode code, String[] params) {
        super(code, params);
    }

    public ScoringApiException(LedpCode code, String[] params, String detailedMessage) {
        super(code, params);
        this.detailedMessage = detailedMessage;
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public void setDetailedMessage(String detailedMessage) {
        this.detailedMessage = detailedMessage;
    }
}
