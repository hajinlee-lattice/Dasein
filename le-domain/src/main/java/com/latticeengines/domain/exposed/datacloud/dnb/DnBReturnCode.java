package com.latticeengines.domain.exposed.datacloud.dnb;

public enum DnBReturnCode {
    OK("Ok"),
    UNMATCH("No matched result found"),
    UNMATCH_TIMEOUT("No matched result found because of timeout"),
    DISCARD("Matched result is discarded"),
    IN_PROGRESS("Batch match is in progress"),
    UNAUTHORIZED("Unauthorized to call API"),
    UNSUBMITTED("Batch request is not submitted because too many requests are waiting for result"),
    RATE_LIMITING("Rate Limiting"),
    TIMEOUT("Timeout"),
    EXPIRED_TOKEN("Token is expired but failed to refresh"),
    EXCEED_REQUEST_NUM("Exceed Hourly Maximum Limit"),
    EXCEED_CONCURRENT_NUM("Exceed Concurrent Limit"),
    BAD_REQUEST("Bad Request"),
    BAD_RESPONSE("Bad Response"),
    UNKNOWN("Unkown error");

    String message;
    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }
}
