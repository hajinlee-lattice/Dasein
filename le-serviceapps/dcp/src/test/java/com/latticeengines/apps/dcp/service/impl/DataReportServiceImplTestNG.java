package com.latticeengines.apps.dcp.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;

import javax.inject.Inject;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.UploadDetails;

public class DataReportServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private DataReportService dataReportService;

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
        when(uploadService.getUploadByUploadId(anyString(), anyString())).thenReturn(uploadDetails);
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

        String report1 = JsonUtils.serialize(dataReportPersist);
        DataReport newReport = getDataReport();
        dataReportService.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, "uploadUID", newReport);
        String report2 = JsonUtils.serialize(dataReportService.getDataReport(mainCustomerSpace,
                DataReportRecord.Level.Upload, "uploadUID"));
        Assert.assertNotEquals(report1, report2);

    }

    private DataReport getDataReport() {
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

        geoDistributionReport.addGeoDistribution("US", "United States of America", ct1, matchCnt);
        geoDistributionReport.addGeoDistribution("AU", "Australia", ct2, matchCnt);
        geoDistributionReport.addGeoDistribution("DE", "Germany", ct3, matchCnt);
        geoDistributionReport.addGeoDistribution("GR", "Greece", ct4, matchCnt);
        geoDistributionReport.addGeoDistribution("MX", "Mexico", ct5, matchCnt);

        DataReport.MatchToDUNSReport matchToDUNSReport = new DataReport.MatchToDUNSReport();
        Long dMatch = new RandomDataGenerator().nextLong(300L, matchCnt);
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

        dataReport.setBasicStats(basicStats);
        dataReport.setInputPresenceReport(inputPresenceReport);
        dataReport.setGeoDistributionReport(geoDistributionReport);
        dataReport.setMatchToDUNSReport(matchToDUNSReport);
        dataReport.setDuplicationReport(duplicationReport);

        dataReport.setRefreshTimestamp(Instant.now().toEpochMilli());

        return dataReport;
    }
}
