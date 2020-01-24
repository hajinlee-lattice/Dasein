package com.latticeengines.common.exposed.csv;

public final class CSVConstants {

    protected CSVConstants() {
        throw new UnsupportedOperationException();
    }
    public static final String CSV_INJECTION_CHARACHTERS = "@+-=";
    // mysql limit string size no more than 64, considering the system prefix 'user_', AvroUtils#getAvroFriendlyString
    // will prefix with 'x' for user field begins with number, so final max length for column header is 57
    public static final int MAX_HEADER_LENGTH = 57;
}
