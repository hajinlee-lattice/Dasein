package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public class DataReportEntityMgrImplTestNG extends DCPFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataReportEntityMgrImpl.class);

    @Inject
    private DataReportEntityMgr dataReportEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testUpdate () {
        DataReportRecord record = new DataReportRecord();
        DataReport.BasicStats basicStats = new DataReport.BasicStats();
        // {"errorCnt": 0, "matchedCnt": 66, "successCnt": 75, "unmatchedCnt": 9, "totalSubmitted": 75, "pendingReviewCnt": 0}
        basicStats.setErrorCnt(0L);
        basicStats.setMatchedCnt(66L);
        basicStats.setSuccessCnt(75L);
        basicStats.setUnmatchedCnt(9L);
        basicStats.setTotalSubmitted(75L);
        basicStats.setPendingReviewCnt(0L);
        record.setBasicStats(basicStats);

        DataReport.DuplicationReport duplicationReport = new DataReport.DuplicationReport();
        // {"uniqueRate": 100, "duplicateRate": 0, "uniqueRecords": 66, "distinctRecords": 66, "duplicateRecords": 0}
        duplicationReport.setUniqueRecords(66L);
        duplicationReport.setDistinctRecords(66L);
        duplicationReport.setDuplicateRecords(0L);
        record.setDuplicationReport(duplicationReport);

        DataReport.GeoDistributionReport geoDistributionReport = new DataReport.GeoDistributionReport();
        // {"geoDistributionList": [{"rate": 100, "count": 75, "geoCode": "US"}]}
        DataReport.GeoDistributionReport.GeographicalItem geographicalItem = new DataReport.GeoDistributionReport.GeographicalItem();
        geographicalItem.setGeoCode("US");
        geographicalItem.setCount(75L);
        geographicalItem.setRate(100.0);
        geoDistributionReport.setGeographicalDistributionList(Collections.singletonList(geographicalItem));
        record.setGeoDistributionReport(geoDistributionReport);

        record.setTenant(mainTestTenant);
        record.setLevel(DataReportRecord.Level.Tenant);
        record.setOwnerId("Upload_pjqloe5t");
        record.setReadyForRollup(Boolean.TRUE);
        record.setRollupStatus(DataReportRecord.RollupStatus.READY);
        dataReportEntityMgr.create(record);

        List<DataReportRecord> all = dataReportEntityMgr.findAll();
        log.info(String.format("Found %d DataReportRecord objects", all.size()));
        Assert.assertEquals(all.size(), 1, "Expected DataReportRecords");
        long notReadyDataRecords = all.stream().filter( rec -> rec.getRollupStatus() != DataReportRecord.RollupStatus.READY ).count();
        Assert.assertEquals(notReadyDataRecords, 0, "Expected all records to be in READY status");  // check if any records are in a not READY status

        String ownerId = record.getOwnerId();
        DataReportRecord preLaunchedRecord = dataReportEntityMgr.findDataReportRecord(DataReportRecord.Level.Tenant, ownerId);
        Assert.assertNotNull(preLaunchedRecord);
        Assert.assertEquals(preLaunchedRecord.getRollupStatus(), DataReportRecord.RollupStatus.READY);
        dataReportEntityMgr.updateDataReportRollupStatus(DataReportRecord.RollupStatus.SUBMITTED, DataReportRecord.Level.Tenant, ownerId);
        DataReportRecord launchedRecord = dataReportEntityMgr.findDataReportRecord(DataReportRecord.Level.Tenant, ownerId);
        Assert.assertNotNull(launchedRecord);
        Assert.assertEquals(launchedRecord.getRollupStatus(), DataReportRecord.RollupStatus.SUBMITTED);

    }
}
