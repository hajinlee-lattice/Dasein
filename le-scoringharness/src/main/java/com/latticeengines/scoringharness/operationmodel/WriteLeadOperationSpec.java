package com.latticeengines.scoringharness.operationmodel;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class WriteLeadOperationSpec extends OperationSpec {
    public WriteLeadOperationSpec(long offset, String externalId, ObjectNode object) {
        super(offset);
        this.externalId = externalId;
        this.object = object;
    }

    // Serialization constructor
    public WriteLeadOperationSpec() {
        super(0);
    }

    public String externalId;

    public ObjectNode object;
}