package com.latticeengines.propdata.match.testframework;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-match-context.xml" })
public abstract class PropDataMatchFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.env}")
    protected String testEnv;

    @Value("${propdata.test.match.client}")
    protected String testMatchClientName;

    @Autowired
    private MetricService metricService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @PostConstruct
    private void postConstruct() {
        metricService.disable();
    }

    protected MatchClient getMatchClient() {
        return MatchClient.valueOf(testMatchClientName);
    }

    protected void switchHdfsPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
    }

    @SuppressWarnings("unchecked")
    protected void uploadDataCsv(String avroDir, String fileName) {
        try {
            URL url = Thread.currentThread().getContextClassLoader()
                    .getResource("com/latticeengines/propdata/match/BulkMatchInput.csv");
            if (url == null) {
                throw new RuntimeException("Cannot find resource BulkMatchInput.csv");
            }
            CSVParser parser = CSVParser.parse(url, Charset.forName("UTF-8"), CSVFormat.DEFAULT);
            List<List<Object>> data = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>(Collections.singleton("ID"));
            int rowNum = 0;
            for (CSVRecord record : parser.getRecords()) {
                if (rowNum == 0) {
                    fieldNames.addAll(IteratorUtils.toList(record.iterator()));
                } else if (record.size() > 0 ){
                    List<Object> row = new ArrayList<>();
                    row.add((int) record.getRecordNumber());
                    for (String field: record) {
                        if ("NULL".equalsIgnoreCase(field) || StringUtils.isEmpty(field)) {
                            row.add(null);
                        } else {
                            row.add(field);
                        }
                    }
                    data.add(row);
                }
                rowNum++;
            }

            uploadAvroData(data, fieldNames, avroDir, fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

    private void uploadAvroData(List<List<Object>> data, List<String> fieldNames, String avroDir, String fileName) {
        List<GenericRecord> records = new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\",\"fields\":["
                + "{\"name\":\"" + fieldNames.get(0) + "\",\"type\":[\"int\",\"null\"]},"
                + "{\"name\":\"" + fieldNames.get(1) + "\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"" + fieldNames.get(2) + "\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"" + fieldNames.get(3) + "\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"" + fieldNames.get(4) + "\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"" + fieldNames.get(5) + "\",\"type\":[\"string\",\"null\"]}"
                + "]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            for (int i = 0; i < tuple.size(); i++) {
                builder.set(fieldNames.get(i), tuple.get(i));
            }
            records.add(builder.build());
        }

        try {
            AvroUtils.writeToLocalFile(schema, records, fileName);
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir + "/" + fileName)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir + "/" + fileName);
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, fileName, avroDir + "/" + fileName);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }

        FileUtils.deleteQuietly(new File(fileName));
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
