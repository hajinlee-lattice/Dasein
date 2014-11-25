package com.latticeengines.skald;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ScoreReader {
    private CSVParser parser;
    
    public ScoreReader(String filename) throws Exception {
        // TODO wrap exceptions nicely
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        parser = new CSVParser(new FileReader(filename), format);
    }

    public List<Map<ScoreType, Object>> read() throws Exception {
        // TODO wrap exceptions nicely
        List<Map<ScoreType,Object>> scores = new ArrayList<Map<ScoreType,Object>>();
        for (CSVRecord record : parser.getRecords()) {
            // TODO Iterate over all possible score outputs and handle appropriately
            String svalue = record.get("probability");
            Double value = Double.parseDouble(svalue);
            Map<ScoreType,Object> row = new HashMap<ScoreType,Object>();
            row.put(ScoreType.PROBABILITY, value);
            scores.add(row);
        }
        
        return scores;
    }
}
