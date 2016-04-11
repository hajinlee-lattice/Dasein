package com.latticeengines.propdata.api.controller;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.service.impl.MatchConstants;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

import edu.emory.mathcs.backport.java.util.Collections;

@Component
public class MatchResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private MatchProxy matchProxy;

    private static final String avroDir = "/tmp/MatchResourceDeploymentTestNG";
    private static final String fileName = "SourceFile_csv.avro";
    private static final String podId = "MatchResourceDeploymentTestNG";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Test(groups = "deployment")
    public void testPredefined() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = matchProxy.matchRealTime(input, true);
        Assert.assertNotNull(output);

        output = matchProxy.matchRealTime(input, false);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment")
    public void testAutoResolvedKeyMap() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, false);
        MatchOutput output = matchProxy.matchRealTime(input, true);
        Assert.assertNotNull(output);

        output = matchProxy.matchRealTime(input, false);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment")
    public void testSingleBlockBulkMatch() throws Exception {
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        MatchInput input = createAvroBulkMatchInput();
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId);
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
    }

    @Test(groups = "deployment")
    public void testMultiBlockBulkMatch() {
        hdfsPathBuilder.changeHdfsPodId(podId);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
        cleanupAvroDir(avroDir);
        uploadTestAVro(avroDir, fileName);
        MatchInput input = createAvroBulkMatchInput();
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId);
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
    }

    private MatchInput createAvroBulkMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(MatchConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.DerivedColumns);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);
        return matchInput;
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


    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration,
                    "com/latticeengines/propdata/match/SourceFile_csv.avro", avroDir + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

    private void cleanupAvroDir(String avroDir) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir);
            }
        } catch (Exception e) {
            Assert.fail("Failed to clean up " + avroDir, e);
        }
    }

}
