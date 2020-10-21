package com.latticeengines.apps.dcp.service.impl;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.UploadDetails;

public class DataReportServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private DataReportService dataReportService;

    @Inject
    private DataReportEntityMgr dataReportEntityMgr;

    private static final Logger log = LoggerFactory.getLogger(DataReportServiceImplTestNG.class);

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        ProjectService projectService = mock(ProjectService.class);
        UploadService uploadService = mock(UploadService.class);
        ProjectInfo projectInfo = new ProjectInfo();
        projectInfo.setProjectId("projectUID");
        UploadDetails uploadDetails = new UploadDetails();
        uploadDetails.setSourceId("sourceUID");
        uploadDetails.setUploadId("uploadUID");
        when(projectService.getProjectBySourceId(anyString(), anyString())).thenReturn(projectInfo);
        when(uploadService.getUploadByUploadId(anyString(), anyString(), anyBoolean())).thenReturn(uploadDetails);
        ReflectionTestUtils.setField(dataReportService, "projectService", projectService);
        ReflectionTestUtils.setField(dataReportService, "uploadService", uploadService);
    }


    @Test(groups = "functional")
    public void testCRUD() {
        DataReport dataReport = getDataReport();
        DataReport.BasicStats basicStats = dataReport.getBasicStats();
        DataReport.InputPresenceReport inputPresenceReport = dataReport.getInputPresenceReport();
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID", inputPresenceReport);
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID", basicStats);
        // verify empty duns count cache
        DunsCountCache cache = dataReportService.getDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload,
                "uploadUID");
        Assert.assertNotNull(cache);
        Assert.assertNull(cache.getDunsCountTableName());
        Assert.assertNull(cache.getSnapshotTimestamp());

        Set<String> subOwnerIds = dataReportService.getChildrenIds(mainCustomerSpace, DataReportRecord.Level.Source,
                "sourceUID");
        Assert.assertTrue(CollectionUtils.isEmpty(subOwnerIds));


        // modify the readyForRollup
        dataReportService.updateReadyForRollup(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID");
        subOwnerIds = dataReportService.getChildrenIds(mainCustomerSpace, DataReportRecord.Level.Source,
                "sourceUID");
        Assert.assertTrue(CollectionUtils.isNotEmpty(subOwnerIds));


        // test find Pid and corresponding duns count table name
        List<Object[]> result = dataReportEntityMgr.findPidAndDunsCountTableName(DataReportRecord.Level.Upload,
                "uploadUID");
        Object[] objs = result.get(0);
        Assert.assertNotNull(objs[0]);
        Assert.assertNull(objs[1]);
        dataReportEntityMgr.countRecordsByDunsCount("test");

        DataReport dataReportPersist = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Upload,  "uploadUID");
        Assert.assertNotNull(dataReportPersist);
        Assert.assertNotNull(dataReportPersist.getInputPresenceReport());
        Assert.assertNotNull(dataReportPersist.getBasicStats());
        Assert.assertNull(dataReportPersist.getGeoDistributionReport());
        Assert.assertNull(dataReportPersist.getMatchToDUNSReport());
        Assert.assertNull(dataReportPersist.getDuplicationReport());

        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID",
                dataReport.getGeoDistributionReport());
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID",
                dataReport.getMatchToDUNSReport());
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID",
                dataReport.getDuplicationReport());
        dataReportService.copyDataReportToParent(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID");

        dataReportPersist = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Upload,  "uploadUID");

        Assert.assertNotNull(dataReportPersist);
        Assert.assertNotNull(dataReportPersist.getInputPresenceReport());
        Assert.assertNotNull(dataReportPersist.getBasicStats());
        Assert.assertNotNull(dataReportPersist.getGeoDistributionReport());
        Assert.assertNotNull(dataReportPersist.getMatchToDUNSReport());
        Assert.assertNotNull(dataReportPersist.getDuplicationReport());

        DataReportRecord uploadReportRecord = dataReportService.getDataReportRecord(mainCustomerSpace,
                DataReportRecord.Level.Upload, "uploadUID");
        DataReportRecord sourceReportRecord = dataReportService.getDataReportRecord(mainCustomerSpace,
                DataReportRecord.Level.Source, "sourceUID");
        DataReportRecord projectReportRecord = dataReportService.getDataReportRecord(mainCustomerSpace,
                DataReportRecord.Level.Project, "projectUID");
        DataReportRecord tenantReportRecord = dataReportService.getDataReportRecord(mainCustomerSpace,
                DataReportRecord.Level.Tenant, CustomerSpace.parse(mainCustomerSpace).toString());
        Assert.assertNotNull(uploadReportRecord);
        Assert.assertNotNull(sourceReportRecord);
        Assert.assertNotNull(projectReportRecord);
        Assert.assertNotNull(tenantReportRecord);
        Assert.assertEquals(uploadReportRecord.getParentId(), sourceReportRecord.getPid());
        Assert.assertEquals(sourceReportRecord.getParentId(), projectReportRecord.getPid());
        Assert.assertEquals(projectReportRecord.getParentId(), tenantReportRecord.getPid());

        DataReport.BasicStats basicStats1 = dataReportService.getDataReportBasicStats(mainCustomerSpace,
                DataReportRecord.Level.Upload, "uploadUID");
        Assert.assertEquals(JsonUtils.serialize(basicStats1), JsonUtils.serialize(dataReportPersist.getBasicStats()));

        DataReport tenantLevelReport = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Tenant, null);
        DataReport projectLevelReport = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Project, "projectUID");
        DataReport sourceLevelReport = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Source, "sourceUID");
        DataReport uploadLevelReport = dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Upload, "uploadUID");

        tenantLevelReport.setRefreshTimestamp(0L);
        projectLevelReport.setRefreshTimestamp(0L);
        sourceLevelReport.setRefreshTimestamp(0L);
        uploadLevelReport.setRefreshTimestamp(0L);
        Assert.assertEquals(JsonUtils.serialize(tenantLevelReport), JsonUtils.serialize(projectLevelReport));
        Assert.assertEquals(JsonUtils.serialize(projectLevelReport), JsonUtils.serialize(sourceLevelReport));
        Assert.assertEquals(JsonUtils.serialize(sourceLevelReport), JsonUtils.serialize(uploadLevelReport));

        dataReportPersist.setRefreshTimestamp(0L);
        String report1 = JsonUtils.serialize(dataReportPersist);
        DataReport newReport = getDataReport();
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID", newReport);
        newReport = dataReportService.getDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID");
        newReport.setRefreshTimestamp(0L);
        String report2 = JsonUtils.serialize(newReport);
        Assert.assertNotEquals(report1, report2);

        Assert.assertEquals(JsonUtils.serialize(tenantLevelReport), JsonUtils.serialize(projectLevelReport));
        Assert.assertEquals(JsonUtils.serialize(projectLevelReport), JsonUtils.serialize(sourceLevelReport));
        Assert.assertEquals(JsonUtils.serialize(sourceLevelReport), report1);
        Assert.assertNotEquals(JsonUtils.serialize(sourceLevelReport), report2);

        DataReport extraReport = getDataReport();
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID2", extraReport);
        Map<String, DataReport.BasicStats> uploadBasicStats =
                dataReportService.getDataReportBasicStatsByParent(mainCustomerSpace, DataReportRecord.Level.Source,
                        "sourceUID");
        Assert.assertEquals(uploadBasicStats.size(), 2);
        Assert.assertTrue(uploadBasicStats.containsKey("uploadUID"));
        Assert.assertTrue(uploadBasicStats.containsKey("uploadUID2"));
        Assert.assertEquals(JsonUtils.serialize(uploadBasicStats.get("uploadUID")), JsonUtils.serialize(newReport.getBasicStats()));

        Map<String, DataReport.BasicStats> projectBasicStats =
                dataReportService.getDataReportBasicStats(mainCustomerSpace, DataReportRecord.Level.Project);
        Assert.assertNotNull(projectBasicStats);
        Assert.assertEquals(projectBasicStats.size(), 1);

        // update ready for rollup for uploadUID2
        dataReportService.updateReadyForRollup(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID2");

        Pageable page = PageRequest.of(0, 10);
        List<Pair<String, Date>> ownerIdToRefreshTime =
                dataReportEntityMgr.getOwnerIdAndTime(DataReportRecord.Level.Tenant, page);
        log.info("data is " + JsonUtils.serialize(ownerIdToRefreshTime));
        Assert.assertTrue(ownerIdToRefreshTime.size() >= 1);

        dataReportService.deleteDataReportUnderOwnerId(mainCustomerSpace, DataReportRecord.Level.Project, "projectUID");
        Set<String> childrenIdsForProject = dataReportService.getChildrenIds(mainCustomerSpace,
                DataReportRecord.Level.Project,
                "projectUID");
        Assert.assertTrue(CollectionUtils.isEmpty(childrenIdsForProject));
        Set<String> childrenIdsForTenant = dataReportService.getChildrenIds(mainCustomerSpace,
                DataReportRecord.Level.Tenant, mainCustomerSpace);
        Assert.assertTrue(CollectionUtils.isEmpty(childrenIdsForTenant));
    }

    public static DataReport getDataReport() {
        DataReport dataReport = new DataReport();

        DataReport.BasicStats basicStats = new DataReport.BasicStats();
        Long totalCnt = new RandomDataGenerator().nextLong(1000L, 3000L);
        Long successCnt = new RandomDataGenerator().nextLong(500L, totalCnt);
        Long errorCnt = totalCnt - successCnt;
        Long matchCnt = new RandomDataGenerator().nextLong(400L, successCnt);
        Long unmatchedCnt = successCnt - matchCnt;

        basicStats.setTotalSubmitted(totalCnt);
        basicStats.setSuccessCnt(successCnt);
        basicStats.setErrorCnt(errorCnt);
        basicStats.setMatchedCnt(matchCnt);
        basicStats.setUnmatchedCnt(unmatchedCnt);
        basicStats.setPendingReviewCnt(0L);

        DataReport.InputPresenceReport inputPresenceReport = new DataReport.InputPresenceReport();
        Long p1 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p2 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p3 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p4 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p5 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p6 = new RandomDataGenerator().nextLong(0L, successCnt);
        inputPresenceReport.addPresence("CompanyName", p1, successCnt);
        inputPresenceReport.addPresence("Country", p2, successCnt);
        inputPresenceReport.addPresence("City", p3, successCnt);
        inputPresenceReport.addPresence("State", p4, successCnt);
        inputPresenceReport.addPresence("Address", p5, successCnt);
        inputPresenceReport.addPresence("Phone", p6, successCnt);

        DataReport.GeoDistributionReport geoDistributionReport = new DataReport.GeoDistributionReport();
        Long ct1 = new RandomDataGenerator().nextLong(1L, matchCnt / 4);
        Long ct2 = new RandomDataGenerator().nextLong(1L, matchCnt / 4);
        Long ct3 = new RandomDataGenerator().nextLong(1L, matchCnt / 4);
        Long ct4 = new RandomDataGenerator().nextLong(1L, matchCnt / 4);
        Long ct5 = matchCnt - ct1 - ct2 - ct3 - ct4;

        geoDistributionReport.addGeoDistribution("US", ct1, matchCnt);
        geoDistributionReport.addGeoDistribution("AU", ct2, matchCnt);
        geoDistributionReport.addGeoDistribution("DE", ct3, matchCnt);
        geoDistributionReport.addGeoDistribution("GR", ct4, matchCnt);
        geoDistributionReport.addGeoDistribution("MX", ct5, matchCnt);

        DataReport.MatchToDUNSReport matchToDUNSReport = new DataReport.MatchToDUNSReport();
        Long dMatch = new RandomDataGenerator().nextLong(300L, matchCnt - 1);
        long dUnMatch = matchCnt - dMatch;
        Long dNoMatch = new RandomDataGenerator().nextLong(0L, dUnMatch);
        matchToDUNSReport.setMatched(dMatch);
        matchToDUNSReport.setUnmatched(dUnMatch);
        matchToDUNSReport.setNoMatchCnt(dNoMatch);
        Long[] c = new Long[11];
        for (int i = 10; i > 0; i--) {
            Long sum = 0L;
            for (int j = 10; j > i; j--) {
                sum += c[j];
            }
            if (i == 1) {
                c[i] = dMatch - sum;
            } else {
                c[i] = new RandomDataGenerator().nextLong(0L, (dMatch - sum) / 2);
            }

        }
        for (int i = 10; i > 0; i--) {
            matchToDUNSReport.addConfidenceItem(i, c[i], dMatch);
        }

        DataReport.DuplicationReport duplicationReport = new DataReport.DuplicationReport();
        Long unique = new RandomDataGenerator().nextLong(100L, matchCnt);
        duplicationReport.setUniqueRecords(unique);
        duplicationReport.setDuplicateRecords(matchCnt - unique);
        duplicationReport.setDistinctRecords(10L);

        dataReport.setBasicStats(basicStats);
        dataReport.setInputPresenceReport(inputPresenceReport);
        dataReport.setGeoDistributionReport(geoDistributionReport);
        dataReport.setMatchToDUNSReport(matchToDUNSReport);
        dataReport.setDuplicationReport(duplicationReport);

        dataReport.setRefreshTimestamp(Instant.now().toEpochMilli());

        return dataReport;
    }
}
