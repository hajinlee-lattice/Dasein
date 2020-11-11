package com.latticeengines.domain.exposed.datacloud;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

public class MatchCoreErrorConstants {
    protected MatchCoreErrorConstants() {}

    public enum ErrorType { MATCH_ERROR, APPEND_ERROR, BATCH_FAILURE }

    // Error codes that should not be treated as match core errors
    public static final Set<String> LOOKUP_NON_ERRORS = ImmutableSet.of("20505", "10002");
    public static final Set<String> APPEND_NON_ERRORS = ImmutableSet.of("40001");

    public static final Map<String, Set<String>> IGNORE_ERRORS = ImmutableMap.of(ErrorType.MATCH_ERROR.name(), LOOKUP_NON_ERRORS, ErrorType.APPEND_ERROR.name(), APPEND_NON_ERRORS);

    public static final Map<String, String> CSV_HEADER_MAP = ImmutableMap.of(
            MatchConstants.MATCH_ERROR_TYPE, "Processing Error Type",
            MatchConstants.MATCH_ERROR_CODE, "Processing Error Code",
            MatchConstants.MATCH_ERROR_INFO, "Processing Error Details"
            );
}
