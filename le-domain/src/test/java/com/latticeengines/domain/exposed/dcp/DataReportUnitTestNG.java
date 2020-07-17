package com.latticeengines.domain.exposed.dcp;

import java.time.Instant;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataReportUnitTestNG {

    @Test(groups = "unit")
    public void testDataReport() {
        DataReport report1 = getDataReport();
        DataReport report2 = getDataReport();

        report1.combineReport(report2);
        Assert.assertNotNull(report1);

        DataReport dataReport = initializeReport();
        dataReport.combineReport(report1);
        // assert report1 = dataReport + report2
        Assert.assertEquals(report1.getBasicStats().getSuccessCnt(),
                dataReport.getBasicStats().getSuccessCnt());
        Assert.assertEquals(report1.getBasicStats().getErrorCnt(), dataReport.getBasicStats().getErrorCnt());
        Assert.assertEquals(report1.getBasicStats().getPendingReviewCnt(), dataReport.getBasicStats().getPendingReviewCnt());
        Assert.assertEquals(report1.getBasicStats().getMatchedCnt(), dataReport.getBasicStats().getMatchedCnt());
        Assert.assertEquals(report1.getBasicStats().getTotalSubmitted(), dataReport.getBasicStats().getTotalSubmitted());
        Assert.assertEquals(report1.getBasicStats().getUnmatchedCnt(), dataReport.getBasicStats().getUnmatchedCnt());

        Assert.assertEquals(JsonUtils.serialize(report1.getGeoDistributionReport()),
                JsonUtils.serialize(dataReport.getGeoDistributionReport()));
        Assert.assertEquals(JsonUtils.serialize(report1.getInputPresenceReport()),
                JsonUtils.serialize(dataReport.getInputPresenceReport()));
        Assert.assertEquals(JsonUtils.serialize(report1.getMatchToDUNSReport()),
                JsonUtils.serialize(dataReport.getMatchToDUNSReport()));



    }

    private DataReport initializeReport() {
        DataReport report = new DataReport();
        report.setInputPresenceReport(new DataReport.InputPresenceReport());
        DataReport.BasicStats stats = new DataReport.BasicStats();
        stats.setSuccessCnt(0L);
        stats.setErrorCnt(0L);
        stats.setPendingReviewCnt(0L);
        stats.setMatchedCnt(0L);
        stats.setTotalSubmitted(0L);
        stats.setUnmatchedCnt(0L);
        report.setBasicStats(stats);
        report.setDuplicationReport(new DataReport.DuplicationReport());
        report.setGeoDistributionReport(new DataReport.GeoDistributionReport());
        report.setMatchToDUNSReport(new DataReport.MatchToDUNSReport());
        return report;
    }

    public DataReport getDataReport() {
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
