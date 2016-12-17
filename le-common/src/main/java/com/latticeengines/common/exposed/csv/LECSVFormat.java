package com.latticeengines.common.exposed.csv;

import org.apache.commons.csv.CSVFormat;

public class LECSVFormat {
    public static final CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',').withIgnoreEmptyLines(true)
            .withIgnoreSurroundingSpaces(true);
}
