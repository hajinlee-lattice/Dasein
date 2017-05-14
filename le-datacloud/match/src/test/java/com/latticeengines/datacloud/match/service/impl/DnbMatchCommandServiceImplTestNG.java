package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DnbMatchCommandService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

public class DnbMatchCommandServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private DnbMatchCommandService dnbMatchService;

    @DataProvider(name = "dnbContextDataProvider")
    private Object[][] dnbMatchContextDataProvider() {
        return new Object[][] {
                { "TestRootOperationUid1", "TestBatchId1", null, DnBReturnCode.SUBMITTED, 5, 4, 1 },
                { "TestRootOperationUid2", "TestBatchId2", null, DnBReturnCode.SUBMITTED, 3, 2, 1 },
                { "TestRootOperationUid3", "TestBatchId3", null, DnBReturnCode.SUBMITTED, 4, 2, 2 },
                { "TestRootOperationUid4", "TestBatchId4", null, DnBReturnCode.SUBMITTED, 5, 3, 2 },
                { "TestRootOperationUid5", "TestBatchId5", "TestBatchId2", DnBReturnCode.SUBMITTED, 5, 4, 1 }, };
    }

    @Test(groups = "functional", dataProvider = "dnbContextDataProvider")
    public void testUpdate(String rootOperationUID, String batchId, String retryForBatchId,
            DnBReturnCode dnbCode, int size, int successRecords, int discardedRecords) {
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
            dnbContexts.put(index + "", dnbContext);
        }
        // Setting failed records
        for (int index = 0; index < discardedRecords; index++) {
            DnBMatchContext dnbContext = new DnBMatchContext();
            dnbContext.setDnbCode(DnBReturnCode.DISCARD);
            dnbContexts.put(index + "", dnbContext);
        }

        dnbBatchContext.setContexts(dnbContexts);
        dnbBatchContext.setTimestamp(new Date());
        dnbMatchService.dnbMatchCommandCreate(dnbBatchContext);
        DnBMatchCommand existingRecord = dnbMatchService.findRecordByField("BatchID",
                dnbBatchContext.getServiceBatchId());
        Assert.assertNotNull(existingRecord);

        // Testing update functionality
        dnbBatchContext.setDnbCode(DnBReturnCode.OK);
        dnbMatchService.dnbMatchCommandUpdate(dnbBatchContext);
        DnBMatchCommand updatedRecord = dnbMatchService.findRecordByField("BatchID",
                dnbBatchContext.getServiceBatchId());
        Assert.assertNotNull(updatedRecord);
        // checking if finish time is populated
        Assert.assertNotNull(updatedRecord.getFinishTime());
        // if DnB batch request is successfully completed
        Assert.assertTrue(updatedRecord.getDnbCode() == DnBReturnCode.OK);
        Assert.assertEquals(updatedRecord.getUnmatchedRecords(),
                (size - successRecords - discardedRecords));
        Assert.assertEquals(updatedRecord.getDuration(),
                ((updatedRecord.getFinishTime().getTime() - updatedRecord.getStartTime().getTime()) / (60 * 1000)));
        Assert.assertEquals(updatedRecord.getBatchId(), batchId);
        Assert.assertNotNull(updatedRecord.getPid());
        Assert.assertNotNull(updatedRecord.getRootOperationUid());

        // if DnB batch request is failed
        dnbBatchContext.setDnbCode(DnBReturnCode.SERVICE_UNAVAILABLE);
        dnbMatchService.dnbMatchCommandUpdate(dnbBatchContext);
        updatedRecord = dnbMatchService.findRecordByField("BatchID",
                dnbBatchContext.getServiceBatchId());
        Assert.assertNotNull(updatedRecord);
        // checking if finish time is populated
        Assert.assertNotNull(updatedRecord.getFinishTime());
        // if DnB batch request is failed
        Assert.assertTrue(updatedRecord.getDnbCode() == DnBReturnCode.SERVICE_UNAVAILABLE);
        Assert.assertEquals(updatedRecord.getUnmatchedRecords(), updatedRecord.getSize());
        Assert.assertEquals(updatedRecord.getAcceptedRecords(), 0);
        Assert.assertEquals(updatedRecord.getDiscardedRecords(), 0);
        Assert.assertEquals(updatedRecord.getDuration(),
                ((updatedRecord.getFinishTime().getTime() - updatedRecord.getStartTime().getTime()) / (60 * 1000)));
        Assert.assertNotNull(updatedRecord.getBatchId());
        Assert.assertNotNull(updatedRecord.getPid());
        Assert.assertNotNull(updatedRecord.getRootOperationUid());

    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        for (int i = 0; i < dnbMatchContextDataProvider().length; i++) {
            Object[] record = dnbMatchContextDataProvider()[i];
            String batchId = (String) record[1];
            DnBMatchCommand dnBBatchRecord = dnbMatchService.findRecordByField("BatchID", batchId);
            dnbMatchService.dnbMatchCommandDelete(dnBBatchRecord);
        }
    }
}
