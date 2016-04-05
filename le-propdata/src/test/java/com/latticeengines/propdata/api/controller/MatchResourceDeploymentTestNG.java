package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    @Test(groups = "deployment", enabled = true)
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

    @Test(groups = "deployment", enabled = true)
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

    @Test(groups = "deployment", enabled = true)
    public void testBulkMatch() {
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
    }

    private MatchInput createAvroBulkMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(MatchConstants.PDTEST_USER));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.DerivedColumns);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
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
