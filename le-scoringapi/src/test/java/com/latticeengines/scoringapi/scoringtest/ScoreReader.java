package com.latticeengines.scoringapi.scoringtest;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.latticeengines.scoringapi.unused.ScoreType;

public class ScoreReader {
    private String path;
    private CSVParser parser;
    
    public ScoreReader(String path) throws Exception {
        try {
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            this.parser = new CSVParser(new FileReader(path), format);
            this.path = path;
        } catch (Exception e) {
            throw new RuntimeException("Failed to open score file " + path + " for CSV parsing.", e);
        }

    }

    public List<Map<ScoreType, Object>> read() throws Exception {
        try {
            List<Map<ScoreType,Object>> scores = new ArrayList<Map<ScoreType,Object>>();
            for (CSVRecord csvrecord : parser.getRecords()) {
               
                Map<ScoreType,Object> row = new HashMap<ScoreType,Object>();
                
                Iterator<Map.Entry<String,String>> it = csvrecord.toMap().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String,String> pair = it.next();
                    String fieldname = pair.getKey();
                    String rawvalue = pair.getValue();
                    ScoreType scoretype = ScoreType.valueOf(fieldname);
                    row.put(scoretype, ScoreType.parse(scoretype, rawvalue));
                }
                scores.add(row);
            }
            
            return scores;
        }
        catch (Exception e) {
            throw new RuntimeException("Issue reading CSV score file " + path, e);
        }
    }
}
