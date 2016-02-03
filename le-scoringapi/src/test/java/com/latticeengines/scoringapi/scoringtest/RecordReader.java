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

import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldType;

public class RecordReader {
    private String path;
    private CSVParser parser;
    private Map<String,FieldSchema> fields;

    public RecordReader(String path, Map<String,FieldSchema> fields) throws Exception {
        try {
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            this.parser = new CSVParser(new FileReader(path), format);
            this.fields = fields;
            this.path = path;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to open record file " + path + " for CSV parsing.", e);
        }
    }

    public List<Map<String,Object>> read() throws Exception {
        try {
            List<Map<String,Object>> records = new ArrayList<Map<String,Object>>();
            for (CSVRecord csvrecord : parser.getRecords()) {
                Map<String,Object> record = new HashMap<String,Object>();

                Iterator<Map.Entry<String,String>> it = csvrecord.toMap().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> pair = it.next();
                    String fieldname = pair.getKey();
                    if (fields.containsKey(fieldname)) {
                        FieldSchema schema = fields.get(fieldname);
                        Object value = FieldType.parse(schema.type, pair.getValue());
                        record.put(fieldname, value);
                    }
                }

                records.add(record);
            }

            return records;
        }
        catch (Exception e) {
            throw new RuntimeException("Issue reading CSV record file " + path, e);
        }
    }
}
