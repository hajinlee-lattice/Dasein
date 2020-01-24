package com.latticeengines.common.exposed.csv;

import org.apache.commons.csv.CSVFormat;

public final class LECSVFormat {

    protected LECSVFormat() {
        throw new UnsupportedOperationException();
    }
    public static final CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',').withIgnoreEmptyLines(true)
            .withIgnoreSurroundingSpaces(true);
}
