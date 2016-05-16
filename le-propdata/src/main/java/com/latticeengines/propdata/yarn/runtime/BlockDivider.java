package com.latticeengines.propdata.yarn.runtime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.latticeengines.common.exposed.util.AvroUtils;

class BlockDivider {

    private static Log log = LogFactory.getLog(BlockDivider.class);
    private String avroPath;
    private List<String> fields;
    private List<Schema.Type> fieldTypes;
    private Configuration yarnConfiguration;
    private Iterator<GenericRecord> iterator;
    private Integer count = 0;
    public Integer groupSize;

    BlockDivider(String avroPath, Configuration yarnConfiguration, Integer groupSize) {
        this.avroPath = avroPath;
        this.yarnConfiguration = yarnConfiguration;
        this.groupSize = groupSize;
        readSchema();
        FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(avroPath));
        iterator = reader.iterator();
    }

    List<String> getFields() {
        return fields;
    }

    private void readSchema() {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        log.info("Got input schema: " + schema.toString());
        fields = new ArrayList<>();
        fieldTypes = new ArrayList<>();
        for (Schema.Field field: schema.getFields()) {
            fields.add(field.name());
            fieldTypes.add(AvroUtils.getType(field));
        }
    }

    List<List<Object>> nextGroup() {
        List<List<Object>> data = new ArrayList<>();
        while (iterator.hasNext() && data.size() < groupSize) {
            GenericRecord record = iterator.next();
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                if (Schema.Type.STRING.equals(fieldTypes.get(i)) && record.get(i) != null) {
                    row.add(record.get(i).toString());
                } else {
                    row.add(record.get(i));
                }
            }
            data.add(row);
        }
        count += data.size();
        log.info("Read a group of " + data.size() + " rows.");
        return data;
    }

    Boolean hasNextGroup() {
        return iterator.hasNext();
    }

    int getCount() {
        return count;
    }

}
