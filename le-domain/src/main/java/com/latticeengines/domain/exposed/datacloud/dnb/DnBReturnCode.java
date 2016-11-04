package com.latticeengines.domain.exposed.datacloud.dnb;

public enum DnBReturnCode {
    Ok("Ok"),
    TimeOut("Timeout"),
    InvalidInput("Invalid Input"),
    NoResult("No Result"),
    Expired("Expired Token"),
    ExceedRequestNum("Exceed Hourly Maximum Limit"),
    ExceedConcurrentNum("Exceed Concurrent Limit"),
    Unknown("Unkown error");

    String message;
    DnBReturnCode(String str) {
        this.message = str;
    }

    public String getMessage() {
        return message;
    }
}
