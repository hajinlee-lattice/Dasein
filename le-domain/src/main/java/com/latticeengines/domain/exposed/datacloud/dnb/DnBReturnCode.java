package com.latticeengines.domain.exposed.datacloud.dnb;

public enum DnBReturnCode {
    OK("Match is finished"), //
    UNMATCH("No matched result found"), //
    UNMATCH_TIMEOUT("No matched result found because of timeout"), //
    DISCARD("Matched result is discarded"), //
    IN_PROGRESS("Batch match is in progress"), //
    UNAUTHORIZED("Unauthorized to call API"), //
    RATE_LIMITING("Rejected by rate limiting service"), //
    TIMEOUT("HTTP timeout"), //
    EXPIRED_TOKEN("Token is expired but failed to refresh"), //
    EXCEED_REQUEST_NUM("Exceed Hourly Maximum Limit"), //
    EXCEED_CONCURRENT_NUM("Exceed Concurrent Limit"), //
    BAD_REQUEST("HTTP bad request"), //
    BAD_RESPONSE("HTTP bad response"), //
    BAD_STATUS("Fail to check batch request status"), //
    SUBMITTED("Batch result has been submitted to DnB"), //
    UNKNOWN("Unknown Status");

    String message;
    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }
}
