package com.latticeengines.scoringharness.operationmodel;

import org.springframework.stereotype.Component;

/**
 * Constructs operations for a particular target system given specs.
 */
@Component
public abstract class OperationFactory {
    public abstract <T extends OperationSpec> Operation<T> getOperation(T spec);
}