package com.latticeengines.pls.service.impl.dcp;

import java.time.Instant;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.pls.service.dcp.DataReportService;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;

@Service("dataReportService")
public class DataReportServiceImpl implements DataReportService {

    private static final Logger log = LoggerFactory.getLogger(DataReportServiceImpl.class);

    @Inject
    private DataReportProxy dataReportProxy;

    @Override
    public DataReport getDataReport(DataReportRecord.Level level, String ownerId, Boolean mock) {
        Preconditions.checkNotNull(level);
        Preconditions.checkArgument(DataReportRecord.Level.Tenant.equals(level) || StringUtils.isNotEmpty(ownerId));
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Preconditions.checkNotNull(customerSpace);
        if (Boolean.TRUE.equals(mock)) {
            return mockReturn();
        }
        return dataReportProxy.getDataReport(customerSpace.toString(), level, ownerId);
    }

    /**
     * Mock return
     */
    private DataReport mockReturn() {
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
        Long p7 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p8 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p9 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p10 = new RandomDataGenerator().nextLong(0L, successCnt);
        Long p11 = new RandomDataGenerator().nextLong(0L, successCnt);
        inputPresenceReport.addPresence("CompanyName", p1, successCnt);
        inputPresenceReport.addPresence("Country", p2, successCnt);
        inputPresenceReport.addPresence("City", p3, successCnt);
        inputPresenceReport.addPresence("State", p4, successCnt);
        inputPresenceReport.addPresence("Address_Street_1", p5, successCnt);
        inputPresenceReport.addPresence("Address_Street_2", p7, successCnt);
        inputPresenceReport.addPresence("PhoneNumber", p6, successCnt);
        inputPresenceReport.addPresence("DUNS", p8, successCnt);
        inputPresenceReport.addPresence("Email", p9, successCnt);
        inputPresenceReport.addPresence("PostalCode", p10, successCnt);
        inputPresenceReport.addPresence("Website", p11, successCnt);

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
        Long dUnMatch = matchCnt - dMatch;
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
