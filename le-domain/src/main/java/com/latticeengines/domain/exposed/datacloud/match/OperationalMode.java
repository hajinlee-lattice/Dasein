package com.latticeengines.domain.exposed.datacloud.match;

public enum OperationalMode {
    LDC_MATCH,
    // CDL match before M25 (Account attributes lookup by AccountId)
    CDL_LOOKUP,
    ENTITY_MATCH,
}
