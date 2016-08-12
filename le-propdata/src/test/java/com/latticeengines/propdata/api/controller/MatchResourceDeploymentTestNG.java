package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
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
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

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
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);

        input.setReturnUnmatched(false);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
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
        uploadDataCsv(avroDir, fileName, "com/latticeengines/propdata/match/BulkMatchInput.csv", fieldTypes);

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
            Thread.sleep(100L);
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
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.RTS);
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
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration,
                    "com/latticeengines/propdata/match/SourceFile_csv.avro", avroDir + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }
}
