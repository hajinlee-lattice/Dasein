package com.latticeengines.domain.exposed.datacloud.dnb;

public enum DnBReturnCode {
    OK("Match is finished"), //
    PARTIAL_SUCCESS("Batch match is partially finished with some error records."), //
    UNMATCH("No match result found"), //
    UNMATCH_TIMEOUT("No match result found because of timeout"), //
    DISCARD("Match result does not meet acceptance criteria, discarded"), //
    IN_PROGRESS("Batch match is in progress"), //
    RATE_LIMITING("Request rejected by rate limiting service"), //
    TIMEOUT("HTTP timeout"), //
    EXPIRED_TOKEN("Token is expired but failed to refresh"), //
    EXCEED_LIMIT_OR_UNAUTHORIZED("Exceeded concurrent/hourly/weekly limit or unauthorized to call API"), //
    BAD_REQUEST("HTTP bad request"), //
    BAD_RESPONSE("HTTP bad response"), //
    BAD_STATUS("Fail to check batch request status"), //
    SUBMITTED("Batch request has been submitted to DnB"), //
    SERVICE_UNAVAILABLE("DnB service is unavailable"), //
    ABANDONED("Batch request is abandoned"), //
    UNKNOWN("Unknown Status");

    String message;
    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }
}
