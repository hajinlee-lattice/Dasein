package com.latticeengines.domain.exposed.datacloud;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class MatchCoreErrorConstants {
    protected MatchCoreErrorConstants() {}

    public static final Set<String> LOOKUP_NON_ERRORS = ImmutableSet.of("05007", "05008", "20505", "10002", "10003");
    public static final Set<String> APPEND_NON_ERRORS = ImmutableSet.of("40001");
}
