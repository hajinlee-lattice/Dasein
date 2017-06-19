package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.entitymgr.DnbMatchCommandEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.service.DnbMatchCommandService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;

public class DnbMatchCommandServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private DnbMatchCommandService dnbMatchService;

    @Autowired
    private DnbMatchCommandEntityMgr dnbMatchEntityMgr;

    @Autowired
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Autowired
    private MatchCommandService matchCommandService;

    @DataProvider(name = "matchCommandDataProvider")
    private Object[][] matchCommandDataProvider() {
        // RootOperationUID, rowsToDnb, rowsMatchedByDnb, duration,
        // dnbDurationAvg
        return new Object[][] { { "TestRootOperationUid3", 14, 10, 0, 0 }, { "TestRootOperationUid4", 0, 0, 0, 0 } };
    }

    @BeforeClass(groups = "functional")
    public void init() {
        for (int i = 0; i < matchCommandDataProvider().length; i++) {
            // populate records
            MatchCommand matchCommand = new MatchCommand();
            Object[] record = matchCommandDataProvider()[i];
            matchCommand.setRootOperationUid((String) (record[0]));
            matchCommand.setRowsRequested(5);
            matchCommand.setMatchStatus(MatchStatus.MATCHING);
            matchCommand.setJobType(MatchRequestSource.MODELING);
            matchCommandEntityMgr.createCommand(matchCommand);
        }
    }

    @DataProvider(name = "dnbContextDataProvider")
    private Object[][] dnbMatchContextDataProvider() {
        // RootOperationUID, BatchID, retryForBatchID, dnbCode, size,
        // successRecords, discardedRecords, updateToStatus
        return new Object[][] {
                { "TestRootOperationUid3", "TestBatchId1", null, DnBReturnCode.SUBMITTED, 5, 4, 1, DnBReturnCode.OK },
                { "TestRootOperationUid2", "TestBatchId2", null, DnBReturnCode.SUBMITTED, 3, 2, 1,
                        DnBReturnCode.SERVICE_UNAVAILABLE },
                { "TestRootOperationUid3", "TestBatchId3", null, DnBReturnCode.SUBMITTED, 4, 2, 2, DnBReturnCode.OK },
                { "TestRootOperationUid4", "TestBatchId4", null, DnBReturnCode.SUBMITTED, 5, 3, 2,
                        DnBReturnCode.SUBMITTED },
                { "TestRootOperationUid3", "TestBatchId5", "TestBatchId2", DnBReturnCode.SUBMITTED, 5, 4, 1,
                        DnBReturnCode.OK } };
    }

    @Test(groups = "functional", dataProvider = "dnbContextDataProvider", priority = 2)
    public void testUpdate(String rootOperationUID, String batchId, String retryForBatchId,
            DnBReturnCode dnbCode, int size, int successRecords, int discardedRecords, DnBReturnCode updateToStatus) {
        // Initializing context
        DnBBatchMatchContext dnbBatchContext = new DnBBatchMatchContext();
        // Testing create functionality
        // Adding test record
        dnbBatchContext.setRootOperationUid(rootOperationUID);
        dnbBatchContext.setServiceBatchId(batchId);
        dnbBatchContext.setRetryForServiceBatchId(retryForBatchId);
        dnbBatchContext.setDnbCode(dnbCode);
        Map<String, DnBMatchContext> dnbContexts = new HashMap<>();
        // Setting successful records
        for (int index = 0; index < successRecords; index++) {
            DnBMatchContext dnbContext = new DnBMatchContext();
            dnbContext.setDnbCode(DnBReturnCode.OK);
            dnbContexts.put(batchId + index + "successful", dnbContext);
        }
        // Setting failed records
        for (int index = 0; index < discardedRecords; index++) {
            DnBMatchContext dnbContext = new DnBMatchContext();
            dnbContext.setDnbCode(DnBReturnCode.DISCARD);
            dnbContexts.put(batchId + index + "failed", dnbContext);
        }
        dnbBatchContext.setContexts(dnbContexts);
        dnbBatchContext.setTimestamp(new Date());
        dnbMatchService.dnbMatchCommandCreate(dnbBatchContext);
        DnBMatchCommand existingRecord = dnbMatchService.findRecordByField("BatchID",
                dnbBatchContext.getServiceBatchId());
        Assert.assertNotNull(existingRecord);
        // updating status
        dnbBatchContext.setDnbCode(updateToStatus);
        dnbMatchService.dnbMatchCommandUpdate(dnbBatchContext);
        DnBMatchCommand updatedRecord = dnbMatchService.findRecordByField("BatchID",
                dnbBatchContext.getServiceBatchId());
        if (updateToStatus == DnBReturnCode.OK) {
            // if DnB batch request is successful
            verifySuccessDnBCommand(updatedRecord, size, successRecords, discardedRecords, batchId);
        }
        if (updateToStatus == DnBReturnCode.SERVICE_UNAVAILABLE) {
            // if DnB batch request is failed
            verifyFailedDnBCommand(updatedRecord);
        }
    }

    public void verifySuccessDnBCommand(DnBMatchCommand updatedRecord, int size, int successRecords,
            int discardedRecords, String batchId) {
        Assert.assertNotNull(updatedRecord);
        // checking if finish time is populated
        Assert.assertNotNull(updatedRecord.getFinishTime());
        // if DnB batch request is successfully completed
        Assert.assertEquals(updatedRecord.getUnmatchedRecords(), (size - successRecords - discardedRecords));
        Assert.assertEquals(updatedRecord.getDuration(),
                ((updatedRecord.getFinishTime().getTime() - updatedRecord.getStartTime().getTime()) / (60 * 1000)));
        Assert.assertEquals(updatedRecord.getBatchId(), batchId);
        Assert.assertNotNull(updatedRecord.getPid());
        Assert.assertNotNull(updatedRecord.getRootOperationUid());
    }

    public void verifyFailedDnBCommand(DnBMatchCommand updatedRecord) {
        Assert.assertNotNull(updatedRecord);
        // checking if finish time is populated
        Assert.assertNotNull(updatedRecord.getFinishTime());
        // if DnB batch request is failed
        Assert.assertEquals(updatedRecord.getUnmatchedRecords(), updatedRecord.getSize());
        Assert.assertEquals(updatedRecord.getAcceptedRecords(), 0);
        Assert.assertEquals(updatedRecord.getDiscardedRecords(), 0);
        Assert.assertEquals(updatedRecord.getDuration(),
                ((updatedRecord.getFinishTime().getTime() - updatedRecord.getStartTime().getTime()) / (60 * 1000)));
        Assert.assertNotNull(updatedRecord.getBatchId());
        Assert.assertNotNull(updatedRecord.getPid());
        Assert.assertNotNull(updatedRecord.getRootOperationUid());
    }

    @Test(groups = "functional", dataProvider = "matchCommandDataProvider", priority = 3)
    public void testFinalize(String rootOperationUid, Integer rowsToDnb, Integer rowsMatchedByDnb, Integer duration,
            Integer dnbDurationAvg) {
        dnbMatchService.finalize(rootOperationUid);
        List<DnBMatchCommand> dnbMatchCommandList = dnbMatchEntityMgr.findAllByField("RootOperationUID",
                rootOperationUid);
        for (DnBMatchCommand dnbBatchRecord : dnbMatchCommandList) {
            Assert.assertNotEquals(dnbBatchRecord.getDnbCode(), DnBReturnCode.SUBMITTED);
        }
    }

    @Test(groups = "functional", dataProvider = "matchCommandDataProvider", priority = 4)
    public void computeDnbStats(String rootOperationUid, Integer rowsToDnb, Integer rowsMatchedByDnb, Integer duration,
            Integer dnbDurationAvg) {
        matchCommandService.update(rootOperationUid)
                .dnbCommands() //
                .commit();
    }

    @Test(groups = "functional", dataProvider = "matchCommandDataProvider", priority = 5)
    public void testUpdatedMatchCommand(String rootOperationUid, Integer rowsToDnb, Integer rowsMatchedByDnb,
            Integer duration,
            Integer dnbDurationAvg) {
        MatchCommand matchCommand = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
        // checking computed columns
        Assert.assertEquals(matchCommand.getRowsToDnb(), rowsToDnb);
        Assert.assertEquals(matchCommand.getRowsMatchedByDnb(), rowsMatchedByDnb);
        Assert.assertEquals(matchCommand.getDuration(), duration);
        Assert.assertEquals(matchCommand.getDnbDurationAvg(), dnbDurationAvg);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        for (int i = 0; i < dnbMatchContextDataProvider().length; i++) {
            Object[] record = dnbMatchContextDataProvider()[i];
            String batchId = (String) record[1];
            DnBMatchCommand dnBBatchRecord = dnbMatchService.findRecordByField("BatchID", batchId);
            dnbMatchService.dnbMatchCommandDelete(dnBBatchRecord);
        }
        for (int i = 0; i < matchCommandDataProvider().length; i++) {
            Object[] record = matchCommandDataProvider()[i];
            String rootOperationUid = (String) record[0];
            MatchCommand matchCommand = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
            matchCommandEntityMgr.deleteCommand(matchCommand);
        }
    }
}
