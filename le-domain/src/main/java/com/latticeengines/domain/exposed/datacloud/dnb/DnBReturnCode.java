package com.latticeengines.domain.exposed.datacloud.dnb;

import java.util.Set;

import com.google.common.collect.Sets;

public enum DnBReturnCode {
    /* bulk job/record & realtime */
    OK("Match is finished"), //

    /* bulk record & realtime */
    UNMATCH("No match result found"), //
    UNMATCH_TIMEOUT("No match result found because of timeout"), //
    DISCARD("Match result does not meet acceptance criteria, discarded"), //

    /* bulk job & realtime */
    RATE_LIMITING("Rate limit exceeded"), //
    UNAUTHORIZED("Unauthorized to call API due to invalid/expired credentials/token"), //
    TIMEOUT("HTTP timeout"), //
    BAD_REQUEST("HTTP bad request"), //
    BAD_RESPONSE("Fail to extract BatchID/status/result from DnB response"), //
    SERVICE_UNAVAILABLE("DnB service is unavailable"), //

    /* bulk job */
    IN_PROGRESS("Batch match is in progress"), //
    SUBMITTED("Batch request has been submitted to DnB"), //
    // If some batch request is retried, and one of try finishes, when the whole
    // match job completes, the other unfinished one is abandoned
    ABANDONED("Batch request is abandoned"), //
    // DnB provides 50+ result/error codes. We don't support parsing all of
    // them. For unparsed ones, mark it as unknown
    UNKNOWN("Unknown Status"), //

    /* Internal status in DnB services. Not exposed */
    PARTIAL_SUCCESS("Batch match is partially finished with some error records.");

    private static final Set<DnBReturnCode> NORMAL_STATUS_CODE = Sets.newHashSet( //
            SUBMITTED, //
            IN_PROGRESS, //
            OK //
    );

    private static final Set<DnBReturnCode> IMMEDIATE_RETRY_CODE = Sets.newHashSet( //
            UNAUTHORIZED
    );

    private static final Set<DnBReturnCode> SCHEDULED_RETRY_CODE = Sets.newHashSet( //
            RATE_LIMITING, //
            UNAUTHORIZED, //
            TIMEOUT, //
            SERVICE_UNAVAILABLE //

    );

    String message;

    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }

    public boolean isSubmittedStatus() {
        return this == SUBMITTED;
    }

    public boolean isNormalStatus() {
        return NORMAL_STATUS_CODE.contains(this);
    }

    public boolean isFinishedStatus() {
        return this == OK;
    }

    public boolean isImmediateRetryStatus() {
        return IMMEDIATE_RETRY_CODE.contains(this);
    }

    public boolean isScheduledRetryStatus() {
        return SCHEDULED_RETRY_CODE.contains(this);
    }
}
