package com.latticeengines.domain.exposed.metadata.transaction;

public enum NullMetricsImputation {
    NULL,
    ZERO, // Replace null with 0 for numerical attrs
    FALSE, // Replace null with false for boolean attrs
}
