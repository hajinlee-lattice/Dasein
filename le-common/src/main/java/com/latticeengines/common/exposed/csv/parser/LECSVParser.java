package com.latticeengines.common.exposed.csv.parser;

import java.io.IOException;

import org.apache.commons.csv.CSVParser;

public class LECSVParser {

    private CSVParser csvParser;

    public CSVParser getCSVParser() {
        return csvParser;
    }

    public void setCSVParser(CSVParser csvParser) {
        this.csvParser = csvParser;
    }

    public void close() throws IOException {
        if (csvParser != null) {
            csvParser.close();
        }
    }
}
