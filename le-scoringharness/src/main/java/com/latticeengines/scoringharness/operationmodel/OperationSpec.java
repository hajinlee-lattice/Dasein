package com.latticeengines.scoringharness.operationmodel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Specifies an operation to perform at a specified offset in time.
 * OperationSpecs, unlike operations, are not tied to the target system.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes({ @Type(value = ReadLeadScoreOperationSpec.class, name = "readscore"),
        @Type(value = WriteLeadOperationSpec.class, name = "write") })
public abstract class OperationSpec {
    protected OperationSpec(long delayMilliseconds) {
        this.offsetMilliseconds = delayMilliseconds;
    }

    /**
     * The delay to wait before performing this operation.
     */
    public long offsetMilliseconds;
}
