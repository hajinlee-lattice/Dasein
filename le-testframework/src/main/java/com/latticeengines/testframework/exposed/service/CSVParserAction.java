package com.latticeengines.testframework.exposed.service;

import org.apache.commons.csv.CSVParser;

public interface CSVParserAction {
    Object parse(CSVParser csvParser);
}
