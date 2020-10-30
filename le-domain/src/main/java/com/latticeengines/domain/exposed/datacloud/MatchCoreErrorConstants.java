package com.latticeengines.domain.exposed.datacloud;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class MatchCoreErrorConstants {
    protected MatchCoreErrorConstants() {}

    public enum ErrorType { MATCH_ERROR, APPEND_ERROR }

    public static final Set<String> LOOKUP_NON_ERRORS = ImmutableSet.of("20505");
    public static final Set<String> APPEND_NON_ERRORS = ImmutableSet.of("40001");

    public static final Map<String, Set<String>> IGNORE_ERRORS = ImmutableMap.of(ErrorType.MATCH_ERROR.name(), LOOKUP_NON_ERRORS, ErrorType.APPEND_ERROR.name(), APPEND_NON_ERRORS);
}
