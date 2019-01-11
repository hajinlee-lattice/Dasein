package com.latticeengines.matchapi.testframework;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.proxy.exposed.matchapi.AMStatsProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

public class MatchapiDeploymentTestNGBase extends MatchapiAbstractTestNGBase {

    @Value("${common.test.matchapi.url}")
    private String hostPort;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient yarnClient;

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    protected AMStatsProxy amStatsProxy;

    @Autowired
    protected ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Override
    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void uploadAvroData(Object[][] data, List<String> fieldNames, List<Class<?>> fieldTypes, String avroDir,
            String fileName) {
        List<List<Object>> list = Arrays.stream(data).map(d -> Arrays.asList(d)).collect(Collectors.toList());
        uploadAvroData(list, fieldNames, fieldTypes, avroDir, fileName);
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
                            Object value = field;
                            try {
                                if (fieldTypes.get(i).equals(Long.class)) {
                                    value = Long.valueOf(field);
                                } else if (fieldTypes.get(i).equals(Integer.class)) {
                                    value = Integer.valueOf(field);
                                }
                            } catch (Exception ex) {
                                System.out.println("Data error=" + ex.getMessage());
                                value = null;
                            }
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

    /*
     * run bulk match job and verify the job finished correctly
     */
    protected MatchCommand runAndVerifyBulkMatch(MatchInput input, String podId) {
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);

        MatchCommand finalStatus = matchProxy.bulkMatchStatus(command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getApplicationId(), appId.toString());
        Assert.assertEquals(finalStatus.getRootOperationUid(), command.getRootOperationUid());
        Assert.assertEquals(finalStatus.getProgress(), 1f);
        Assert.assertEquals(finalStatus.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(finalStatus.getResultLocation(),
                hdfsPathBuilder.constructMatchOutputDir(command.getRootOperationUid()).toString());
        return finalStatus;
    }

}
