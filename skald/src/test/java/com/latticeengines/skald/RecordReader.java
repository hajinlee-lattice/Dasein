package com.latticeengines.skald;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.latticeengines.skald.model.FieldSchema;

public class RecordReader {
    private CSVParser parser;
    private Map<String,FieldSchema> fields;
    
    public RecordReader(String path, Map<String,FieldSchema> fields) throws Exception {
        // TODO wrap exceptions nicely
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        this.parser = new CSVParser(new FileReader(path), format);
        this.fields = fields;
    }
    
    public List<Map<String,Object>> read() throws Exception {
        // TODO wrap exceptions nicely
        List<Map<String,Object>> records = new ArrayList<Map<String,Object>>();
        for (CSVRecord csvrecord : parser.getRecords()) {
            Map<String,Object> record = new HashMap<String,Object>();
            
            Iterator<Map.Entry<String,String>> it = csvrecord.toMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> pair = it.next();
                String fieldname = pair.getKey();
                if (fields.containsKey(fieldname)) {
                    FieldSchema schema = fields.get(fieldname);
                    Object value = getTypedValue(schema, pair.getValue());
                    record.put(fieldname, value);
                }
            }
            
            records.add(record);
        }
        
        return records;
    }
    
    private Object getTypedValue(FieldSchema schema, String rawvalue) {
        if (rawvalue == null) {
            return null;
        }
        
        // TODO instead use java reflection to call parse using the correct type
        switch (schema.type){
        case BOOLEAN:
            return Boolean.parseBoolean(rawvalue);
        case FLOAT:
            return Double.parseDouble(rawvalue);
        case INTEGER:
            return Integer.parseInt(rawvalue);
        case STRING:
            return rawvalue;
        case TEMPORAL:
            return Long.parseLong(rawvalue);
        default:
            throw new UnsupportedOperationException("Unsupported field type " + schema.type);
        }
    }
}
