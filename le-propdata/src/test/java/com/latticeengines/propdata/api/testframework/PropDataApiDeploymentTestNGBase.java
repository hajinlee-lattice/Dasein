package com.latticeengines.propdata.api.testframework;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;

public abstract class PropDataApiDeploymentTestNGBase extends PropDataApiAbstractTestNGBase {

    @Value("${propdata.api.deployment.hostport}")
    private String hostPort;

    @Autowired
    protected Configuration yarnConfiguration;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void uploadAvroData(List<List<Object>> data, List<String> fieldNames, List<Class<?>> fieldTypes,
            String avroDir, String fileName) {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            schemaMap.put(fieldNames.get(i), fieldTypes.get(i));
        }
        String tableName = fileName.endsWith(".avro") ? fileName.replace(".avro", "") : fileName;
        Schema schema = AvroUtils.constructSchema(tableName, schemaMap);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            for (int i = 0; i < tuple.size(); i++) {
                builder.set(fieldNames.get(i), tuple.get(i));
            }
            records.add(builder.build());
        }

        try {
            String fullPath = new Path(avroDir).append(fileName).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, fullPath)) {
                HdfsUtils.rmdir(yarnConfiguration, fullPath);
            }
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fullPath, records);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected void uploadDataCsv(String avroDir, String fileName, String csvFile, List<Class<?>> fieldTypes,
            String IdKey) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(csvFile);
            if (url == null) {
                throw new RuntimeException("Cannot find resource file=" + csvFile);
            }
            CSVParser parser = CSVParser.parse(url, Charset.forName("UTF-8"), CSVFormat.DEFAULT);
            List<List<Object>> data = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>(Collections.singleton(IdKey));
            int rowNum = 0;
            for (CSVRecord record : parser.getRecords()) {
                if (rowNum == 0) {
                    fieldNames.addAll(IteratorUtils.toList(record.iterator()));
                } else if (record.size() > 0) {
                    List<Object> row = new ArrayList<>();
                    row.add((int) record.getRecordNumber());
                    int i = 1;
                    for (String field : record) {
                        if ("NULL".equalsIgnoreCase(field) || StringUtils.isEmpty(field)) {
                            row.add(null);
                        } else {
                            Object value = getValue(fieldTypes, i, field);
                            row.add(value);
                        }
                        i++;
                    }
                    data.add(row);
                }
                rowNum++;
            }
            uploadAvroData(data, fieldNames, fieldTypes, avroDir, fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

    private Object getValue(List<Class<?>> fieldTypes, int i, String field) {
        Object value = field;
        try {
            if (fieldTypes.get(i).equals(Long.class)) {
                value = Long.valueOf(field);
            } else if (fieldTypes.get(i).equals(Integer.class)) {
                value = Integer.valueOf(field);
            } else if (fieldTypes.get(i).equals(Double.class)) {
                value = Double.valueOf(field);
            } else if (fieldTypes.get(i).equals(Boolean.class)) {
                value = Boolean.valueOf(field);
            }
        } catch (Exception ex) {
            value = null;
        }
        return value;
    }

    protected List<String> getFieldNamesFromCSVFile(String csvFile) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(csvFile);
            if (url == null) {
                throw new RuntimeException("Cannot find resource file=" + csvFile);
            }
            CSVParser parser = CSVParser.parse(url, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withHeader());
            Map<String, Integer> columnMap = parser.getHeaderMap();
            List<String> fieldNames = new ArrayList<>();
            for (String key : columnMap.keySet()) {
                fieldNames.add(key);
            }
            return fieldNames;

        } catch (Exception ex) {
            throw new RuntimeException("Failed to get field names.", ex);
        }
    }

    protected void cleanupAvroDir(String avroDir) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir);
            }
        } catch (Exception e) {
            Assert.fail("Failed to clean up " + avroDir, e);
        }
    }

}
