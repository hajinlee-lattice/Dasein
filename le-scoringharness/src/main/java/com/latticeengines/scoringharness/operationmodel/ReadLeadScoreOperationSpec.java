package com.latticeengines.scoringharness.operationmodel;

import java.util.List;

public class ReadLeadScoreOperationSpec extends OperationSpec {
    public ReadLeadScoreOperationSpec(long offset, String externalId, List<String> additionalFields) {
        super(offset);
        this.externalId = externalId;
        this.additionalFields = additionalFields;
    }

    // Serialization constructor
    public ReadLeadScoreOperationSpec() {
        super(0);
    }

    public String externalId;

    public List<String> additionalFields;
}