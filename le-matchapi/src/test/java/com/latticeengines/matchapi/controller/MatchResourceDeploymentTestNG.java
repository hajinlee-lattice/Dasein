package com.latticeengines.matchapi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.matchapi.testframework.TestMatchInputUtils;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.service.MatchCommandService;

@Component
public class MatchResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final String avroDir = "/tmp/MatchResourceDeploymentTestNG";
    private static final String fileName = "SourceFile_csv.avro";
    private static final String podId = "MatchResourceDeploymentTestNG";
    private static List<String> domains = new ArrayList<>();

    static {
        domains.add("fb.com");
        domains.add("google.com");
        domains.add("salesforce.com");
        domains.add("microsoft.com");
        domains.add("ibm.com");
        domains.add("dnb.com");
        domains.add("wikipedia.com");
        domains.add("amazon.com");
        domains.add("wipro.com");
        domains.add("apple.com");
        domains.add("apache.com");
    }

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Test(groups = "deployment")
    public void testPredefined() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);

        input.setReturnUnmatched(false);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment", enabled = true)
    public void testAccountMasterRTSMatch() {
        testAccountMasterRTSMatch(true, "a@fb.com", true, false, null);
        testAccountMasterRTSMatch(true, "a@salesforce.com", false, true, "null");
        testAccountMasterRTSMatch(true, "a@dnb.com", true, true, "NULL");
    }

    private void testAccountMasterRTSMatch(boolean resolveKeyMap, String domain, boolean isEmail,
            boolean setUnionSelection, String duns) {
        MatchInput input = prepareSimpleMatchInput(resolveKeyMap, domain, isEmail, setUnionSelection, duns);
        input.setDataCloudVersion("2.0.0");
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);

        input.setReturnUnmatched(false);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        int idx = 0;
        for (Object res : output.getResult().get(0).getOutput()) {
            String field = output.getOutputFields().get(idx++);
            if (!StringUtils.isEmpty(res)) {
                System.out.print(field + " = " + res + ", ");
            }
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testAccountMasterRTSBulkMatch() throws IOException {
        int size = 200;
        List<MatchInput> inputList = prepareBulkMatchInput(size, "2.0.0", true);

        BulkMatchInput input = new BulkMatchInput();
        input.setHomogeneous(false);
        input.setInputList(inputList);

        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(input));

        long startLookup = System.currentTimeMillis();
        BulkMatchOutput output = matchProxy.matchRealTime(input);
        System.out.println("Time taken to do dnb based AM lookup for " + size + " entries (with "
                + (size > domains.size() ? domains.size() : size) + " unique domains) = "
                + (System.currentTimeMillis() - startLookup) + " millis");
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getOutputList());
        Assert.assertEquals(output.getOutputList().size(), size);

        for (MatchOutput outputRecord : output.getOutputList()) {
            Assert.assertNotNull(outputRecord);
            Assert.assertTrue(outputRecord.getResult().size() > 0);
            Assert.assertTrue(outputRecord.getResult().get(0).isMatched());
        }
    }

    private List<MatchInput> prepareBulkMatchInput(int count, String dataCloudVersion, boolean resolveKeyMap) {
        List<MatchInput> inputList = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            MatchInput input = prepareSimpleMatchInput(resolveKeyMap, //
                    domains.get(i % domains.size()), //
                    false, true, null);
            input.setReturnUnmatched(false);
            input.setDataCloudVersion(dataCloudVersion);
            inputList.add(input);
        }

        return inputList;
    }

    private MatchInput prepareSimpleMatchInput(boolean resolveKeyMap, String domain, boolean isEmail,
            boolean setUnionSelection, String duns) {
        Object[][] data = new Object[][] { { 0, domain, duns } };
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        MatchInput input = new MatchInput();
        input.setReturnUnmatched(true);

        if (setUnionSelection) {
            UnionSelection us = new UnionSelection();
            Map<Predefined, String> predefinedSelections = new HashMap<>();
            predefinedSelections.put(Predefined.RTS, "2.0");
            us.setPredefinedSelections(predefinedSelections);
            input.setUnionSelection(us);
        } else {
            input.setPredefinedSelection(Predefined.RTS);
        }
        input.setTenant(new Tenant("PD_Test"));
        List<String> fields = Arrays.asList("ID", isEmail ? "Email" : "Domain", "DUNS");
        input.setFields(fields);
        if (resolveKeyMap) {
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(fields));
        }
        input.setData(mockData);
        input.setReturnUnmatched(true);
        return input;
    }

    @Test(groups = "deployment")
    public void testAutoResolvedKeyMap() {
        List<List<Object>> data = TestMatchInputUtils.getGoodInputData();
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);

        input.setReturnUnmatched(false);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "deployment")
    public void testBulkMatchWithSchema() throws Exception {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(avroDir);
        List<Class<?>> fieldTypes = new ArrayList<>();
        fieldTypes.add(Integer.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        fieldTypes.add(String.class);
        uploadDataCsv(avroDir, fileName, "matchinput/BulkMatchInput.csv", fieldTypes, "ID");

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroDir + "/" + fileName));

        // use avr dir and with schema
        MatchInput input = createAvroBulkMatchInput(true, schema);
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

        Assert.assertEquals(finalStatus.getRowsMatched(), new Integer(100));

        // use avro file and without schema
        input = createAvroBulkMatchInput(false, null);
        input.setExcludePublicDomains(true);
        command = matchProxy.matchBulk(input, podId);
        appId = ConverterUtils.toApplicationId(command.getApplicationId());
        status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
        Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);
        Assert.assertEquals(matchCommand.getRowsMatched(), new Integer(99));
    }

    @Test(groups = "deployment")
    public void testMultiBlockBulkMatch() throws InterruptedException {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
        cleanupAvroDir(avroDir);
        uploadTestAVro(avroDir, fileName);

        MatchInput input = createAvroBulkMatchInput(true, null);
        // input.setExcludePublicDomains(true);
        MatchCommand command = matchProxy.matchBulk(input, podId);
        ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());

        // mimic one block failed
        while (command.getMatchBlocks() == null || command.getMatchBlocks().isEmpty()) {
            Thread.sleep(1000L);
            command = matchProxy.bulkMatchStatus(command.getRootOperationUid());
        }
        String blockAppId = command.getMatchBlocks().get(0).getApplicationId();
        YarnUtils.kill(yarnConfiguration, ConverterUtils.toApplicationId(blockAppId));

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

    private MatchInput createAvroBulkMatchInput(boolean useDir, Schema inputSchema) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
        matchInput.setPredefinedSelection(Predefined.RTS);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        if (useDir) {
            inputBuffer.setAvroDir(avroDir);
        } else {
            inputBuffer.setAvroDir(avroDir + "/" + fileName);
        }
        if (inputSchema != null) {
            inputBuffer.setSchema(inputSchema);
        }
        matchInput.setInputBuffer(inputBuffer);
        return matchInput;
    }

    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, "matchinput/SourceFile_csv.avro",
                    avroDir + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }
}
