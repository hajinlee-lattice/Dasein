package com.latticeengines.datacloud.match.dnb;

public enum DnBReturnCode {
    OK("Ok"),
    DISCARD("Discard"),
    IN_PROGRESS("In Progress"),
    RATE_LIMITING("Rate Limiting"),
    TIMEOUT("Timeout"),
    INVALID_INPUT("Invalid Input"),
    NO_RESULT("No Result"),
    EXPIRED("Expired Token"),
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
