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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.TravelLog;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
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
    private static final Timeout BATCH_TIMEOUT = new Timeout(new FiniteDuration(1, TimeUnit.HOURS));

    @Autowired
    private MatchActorSystem actorSystem;

    @Value("${datacloud.match.default.log.level.realtime:DEBUG}")
    private Level defaultRealTimeLogLevel;

    @Value("${datacloud.match.default.log.level.bulk:INFO}")
    private Level defaultBulkLogLevel;

    @Override
    public <T extends OutputRecord> void callMatch(List<T> matchRecords, String rootOperationUid,
            String dataCloudVersion, String decisionGraph, Level logLevel) throws Exception {
        checkRecordType(matchRecords);

        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        List<Future<Object>> matchFutures = new ArrayList<>();

        for (T record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            if (!StringUtils.isEmpty(matchRecord.getLatticeAccountId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                MatchTraveler travelContext = new MatchTraveler(rootOperationUid, matchKeyTuple);
                matchRecord.setTravelerId(travelContext.getTravelerId());
                travelContext.setDataCloudVersion(dataCloudVersion);
                if (StringUtils.isNotEmpty(decisionGraph)) {
                    travelContext.setDecisionGraph(decisionGraph);
                }

                matchFutures.add(askFuzzyMatchAnchor(travelContext, timeout));
            }
        }

        List<FuzzyMatchHistory> fuzzyMatchHistories = new ArrayList<>();
        List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
        if (logLevel == null) {
            logLevel = actorSystem.isBatchMode() ? defaultBulkLogLevel : defaultRealTimeLogLevel;
        }
        for (int idx = 0; idx < matchFutures.size(); idx++) {
            Future<Object> future = matchFutures.get(idx);
            if (future != null) {
                // null future means already has lattice account id, or failed
                // in initialization
                MatchTraveler traveler = (MatchTraveler) Await.result(future, timeout.duration());
                InternalOutputRecord matchRecord = (InternalOutputRecord) matchRecords.get(idx);
                matchRecord.setLatticeAccountId((String) traveler.getResult());
                fuzzyMatchHistories.add(new FuzzyMatchHistory(traveler));
                if (traveler.getDnBMatchOutput() != null) {
                    dnBMatchHistories.add(new DnBMatchHistory(traveler));
                }
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
            }
        }

        writeFuzzyMatchHistory(fuzzyMatchHistories);
        writeDnBMatchHistory(dnBMatchHistories);
    }

    @MatchStep
    private void writeFuzzyMatchHistory(List<FuzzyMatchHistory> metrics) {
        try {
            MeasurementMessage<FuzzyMatchHistory> message = new MeasurementMessage<>();
            message.setMeasurements(metrics);
            message.setMetricDB(MetricDB.LDC_Match);
            actorSystem.getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }

    @MatchStep
    private void writeDnBMatchHistory(List<DnBMatchHistory> metrics) {
        try {
            MeasurementMessage<DnBMatchHistory> message = new MeasurementMessage<>();
            message.setMeasurements(metrics);
            message.setMetricDB(MetricDB.LDC_Match);
            actorSystem.getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }

    private Future<Object> askFuzzyMatchAnchor(MatchTraveler traveler, Timeout timeout) {
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
                if (Level.DEBUG.equals(logEntry.getLevel()) || Level.INFO.equals(logEntry.getLevel())) {
                    record.log(logEntry.getMessage());
                } else if (Level.WARN.equals(logEntry.getLevel())) {
                    if (logEntry.getThrowable() == null) {
                        record.log(logEntry.getMessage());
                    } else {
                        record.log(logEntry.getMessage() + "\n"
                                + StringEscapeUtils.escapeJson(ExceptionUtils.getFullStackTrace(logEntry.getThrowable())));
                    }
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
            matchKeyTuple.setZipcode(nameLocationInfo.getZipCode());
            matchKeyTuple.setPhoneNumber(nameLocationInfo.getPhoneNumber());
        }
        if (!matchRecord.isPublicDomain()) {
            matchKeyTuple.setDomain(matchRecord.getParsedDomain());
        }
        matchKeyTuple.setDuns(matchRecord.getParsedDuns());
        return matchKeyTuple;
    }
}
