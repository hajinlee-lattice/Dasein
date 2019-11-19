package com.latticeengines.common.exposed.csv;

public class CSVConstants {
    public static final String CSV_INJECTION_CHARACHTERS = "@+-=";
    // mysql limit string size no more than 64, considering the system prefix 'user_', max length for column header
    // is 58
    public static final int MAX_HEADER_LENGTH = 58;
}
