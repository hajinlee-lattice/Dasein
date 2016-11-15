package com.latticeengines.datacloud.match.dnb;

public enum DnBReturnCode {
    OK("Ok"),
    UNMATCH("No matched result found"),
    DISCARD("Matched result is discarded"),
    IN_PROGRESS("Batch match is in progress"),
    UNAUTHORIZED("Unauthorized to call API"),
    RATE_LIMITING("Rate Limiting"),
    TIMEOUT("Timeout"),
    EXPIRED("Token is expired but failed to refresh"),
    EXCEED_REQUEST_NUM("Exceed Hourly Maximum Limit"),
    EXCEED_CONCURRENT_NUM("Exceed Concurrent Limit"),
    BAD_REQUEST("Bad Request"),
    UNKNOWN("Unkown error");

    String message;
    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }
}
