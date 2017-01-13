package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.TravelLog;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class FuzzyMatchServiceImpl implements FuzzyMatchService {
    private static final Log log = LogFactory.getLog(FuzzyMatchServiceImpl.class);

    private static final Timeout REALTIME_TIMEOUT = new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
    private static final Timeout BATCH_TIMEOUT = new Timeout(new FiniteDuration(3, TimeUnit.HOURS));

    @Autowired
    private MatchActorSystem actorSystem;

    @Override
    public <T extends OutputRecord> void callMatch(List<T> matchRecords, String rootOperationUid,
            String dataCloudVersion, String decisionGraph, Level logLevel, boolean useDnBCache, boolean useRemoteDnB,
            boolean logDnBBulkResult)
            throws Exception {
        checkRecordType(matchRecords);
        logLevel = setLogLevel(logLevel);
        List<Future<Object>> matchFutures = callMatchInternal(matchRecords, rootOperationUid, dataCloudVersion,
                decisionGraph, logLevel, useDnBCache, useRemoteDnB, logDnBBulkResult);

        fetchIdResult(matchRecords, logLevel, matchFutures);
    }

    @Override
    public <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, String rootOperationUid,
            String dataCloudVersion, String decisionGraph, Level logLevel, boolean useDnBCache, boolean useRemoteDnB,
            boolean logDnBBulkResult)
            throws Exception {
        logLevel = setLogLevel(logLevel);
        return callMatchInternal(matchRecords, rootOperationUid, dataCloudVersion, decisionGraph, logLevel, useDnBCache,
                useRemoteDnB, logDnBBulkResult);
    }

    @Override
    public <T extends OutputRecord> void fetchIdResult(List<T> matchRecords, Level logLevel,
            List<Future<Object>> matchFutures) throws Exception {
        logLevel = setLogLevel(logLevel);
        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        List<FuzzyMatchHistory> fuzzyMatchHistories = new ArrayList<>();
        for (int idx = 0; idx < matchFutures.size(); idx++) {
            Future<Object> future = matchFutures.get(idx);
            if (future != null) {
                // null future means already has lattice account id, or failed
                // in initialization
                MatchTraveler traveler = (MatchTraveler) Await.result(future, timeout.duration());
                InternalOutputRecord matchRecord = (InternalOutputRecord) matchRecords.get(idx);
                String result = (String) traveler.getResult();
                matchRecord.setLatticeAccountId(result);
                traveler.setBatchMode(actorSystem.isBatchMode());
                fuzzyMatchHistories.add(new FuzzyMatchHistory(traveler));
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
            }
        }

        writeFuzzyMatchHistory(fuzzyMatchHistories);
    }

    private Level setLogLevel(Level logLevel) {
        if (logLevel == null) {
            logLevel = Level.INFO;
        }
        return logLevel;
    }

    private <T extends OutputRecord> List<Future<Object>> callMatchInternal(List<T> matchRecords,
            String rootOperationUid, String dataCloudVersion, String decisionGraph, Level logLevel, boolean useDnBCache,
            boolean useRemoteDnB, boolean logDnBBulkResult) {

        List<Future<Object>> matchFutures = new ArrayList<>();
        for (T record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            if (StringUtils.isNotEmpty(matchRecord.getLatticeAccountId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                MatchTraveler travelContext = new MatchTraveler(rootOperationUid, matchKeyTuple);
                travelContext.setLogLevel(logLevel);
                matchRecord.setTravelerId(travelContext.getTravelerId());
                travelContext.setDataCloudVersion(dataCloudVersion);
                if (StringUtils.isNotEmpty(decisionGraph)) {
                    travelContext.setDecisionGraph(decisionGraph);
                }
                travelContext.setUseDnBCache(useDnBCache);
                travelContext.setUseRemoteDnB(useRemoteDnB);
                travelContext.setLogDnBBulkResult(logDnBBulkResult);
                matchFutures.add(askFuzzyMatchAnchor(travelContext));
            }
        }
        return matchFutures;
    }

    @MatchStep
    private void writeFuzzyMatchHistory(List<FuzzyMatchHistory> metrics) {
        try {
            MeasurementMessage<FuzzyMatchHistory> message = new MeasurementMessage<>();
            message.setMeasurements(metrics);
            message.setMetricDB(MetricDB.LDC_Match);
            actorSystem.getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    private Future<Object> askFuzzyMatchAnchor(MatchTraveler traveler) {
        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        return Patterns.ask(actorSystem.getFuzzyMatchAnchor(), traveler, timeout);
    }

    private void checkRecordType(List<? extends OutputRecord> matchRequests) {
        if (matchRequests.size() > actorSystem.getMaxAllowedRecordCount()) {
            throw new RuntimeException("Too many records in the request: " + matchRequests.size()
                    + ", max allowed record count = " + actorSystem.getMaxAllowedRecordCount());
        }
        for (OutputRecord matchRequest : matchRequests) {
            if (!(matchRequest instanceof InternalOutputRecord)) {
                throw new RuntimeException("Expected request of type " + InternalOutputRecord.class);
            }
        }
    }

    private void dumpTravelStory(InternalOutputRecord record, MatchTraveler traveler, Level level) {
        for (TravelLog logEntry : traveler.getTravelStory()) {
            if (logEntry.getLevel().isGreaterOrEqual(level)) {
                if (logEntry.getThrowable() == null) {
                    record.log(logEntry.getMessage());
                } else {
                    record.log(logEntry.getMessage() + "\n"
                            + StringEscapeUtils.escapeJson(ExceptionUtils.getFullStackTrace(logEntry.getThrowable())));
                }
            }
        }
    }

    private MatchKeyTuple createMatchKeyTuple(InternalOutputRecord matchRecord) {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        NameLocation nameLocationInfo = matchRecord.getParsedNameLocation();
        if (nameLocationInfo != null) {
            matchKeyTuple.setCity(nameLocationInfo.getCity());
            matchKeyTuple.setCountry(nameLocationInfo.getCountry());
            matchKeyTuple.setCountryCode(nameLocationInfo.getCountryCode());
            matchKeyTuple.setName(nameLocationInfo.getName());
            matchKeyTuple.setState(nameLocationInfo.getState());
            matchKeyTuple.setZipcode(nameLocationInfo.getZipcode());
            matchKeyTuple.setPhoneNumber(nameLocationInfo.getPhoneNumber());
        }
        if (!matchRecord.isPublicDomain() || matchRecord.isMatchEvenIsPublicDomain()) {
            matchKeyTuple.setDomain(matchRecord.getParsedDomain());
        }
        matchKeyTuple.setDuns(matchRecord.getParsedDuns());
        return matchKeyTuple;
    }
}
