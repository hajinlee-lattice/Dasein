package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.TravelLog;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class FuzzyMatchServiceImpl implements FuzzyMatchService {

    private static final Logger log = LoggerFactory.getLogger(FuzzyMatchServiceImpl.class);

    private static final Timeout REALTIME_TIMEOUT = new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
    private static final Timeout BATCH_TIMEOUT = new Timeout(new FiniteDuration(3, TimeUnit.HOURS));

    @Autowired
    private MatchActorSystem actorSystem;

    @Value("${datacloud.match.publish.match.history:false}")
    private boolean isMatchHistoryEnabled;

    @Autowired
    private DomainCollectService domainCollectService;

    @Override
    public <T extends OutputRecord> void callMatch(List<T> matchRecords, MatchInput matchInput) throws Exception {
        checkRecordType(matchRecords);
        Level logLevel = setLogLevel(matchInput.getLogLevelEnum());
        matchInput.setLogLevelEnum(logLevel);
        List<Future<Object>> matchFutures = callMatchInternal(matchRecords, matchInput);
        fetchIdResult(matchRecords, logLevel, matchFutures);
    }

    @Override
    public <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, MatchInput matchInput)
            throws Exception {
        Level logLevel = setLogLevel(matchInput.getLogLevelEnum());
        matchInput.setLogLevelEnum(logLevel);
        return callMatchInternal(matchRecords, matchInput);
    }

    @MatchStep
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
                if (!traveler.getMatchInput().isCdlMatch()) {
                    matchRecord.setCdlId(result);
                }
                if (StringUtils.isNotEmpty(result)) {
                    matchRecord.setMatched(true);
                }
                if (StringUtils.isNotEmpty(traveler.getMatchKeyTuple().getDuns())) {
                    matchRecord.setMatchedDuns(traveler.getMatchKeyTuple().getDuns());
                } else if (Level.DEBUG.equals(logLevel)) {
                    // might be the case of low quality duns

                    List<DnBMatchContext> dnBMatchContexts = traveler.getDnBMatchContexts();
                    List<String> dnbCacheIds = new ArrayList<>();

                    if (dnBMatchContexts != null && !dnBMatchContexts.isEmpty()) {
                        for (DnBMatchContext dnBMatchContext : dnBMatchContexts) {
                            String cacheId = dnBMatchContext.getCacheId();
                            if (StringUtils.isNotEmpty(cacheId)) {
                                dnbCacheIds.add(cacheId);
                            }
                        }
                    }
                    if (!dnbCacheIds.isEmpty()) {
                        matchRecord.setDnbCacheIds(dnbCacheIds);
                    }
                }
                setDnbReturnCode(traveler, matchRecord);
                setDebugValues(traveler, matchRecord);
                traveler.setBatchMode(actorSystem.isBatchMode());
                fuzzyMatchHistories.add(new FuzzyMatchHistory(traveler));
                if (isMatchHistoryEnabled)
                    matchRecord.setFabricMatchHistory(getDnbMatchHistory(matchRecord, traveler));
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
            }
        }

        writeFuzzyMatchHistory(fuzzyMatchHistories);
    }

    private MatchHistory getDnbMatchHistory(InternalOutputRecord matchRecord, MatchTraveler traveler) {
        MatchHistory matchHistory = matchRecord.getFabricMatchHistory();
        matchHistory.setMatched(traveler.isMatched()).setMatchMode(traveler.getMode());
        if (CollectionUtils.isNotEmpty(traveler.getDnBMatchContexts())) {
            DnBMatchContext dnbMatchContext = traveler.getDnBMatchContexts().get(0);
            if (dnbMatchContext != null) {
                matchHistory.withDnBMatchResult(dnbMatchContext);
            }
        }

        if (traveler.getTotalTravelTime() != null)
            matchHistory.setMatchRetrievalTime("" + traveler.getTotalTravelTime());

        return matchHistory;
    }

    private void setDnbReturnCode(MatchTraveler traveler, InternalOutputRecord matchRecord) {
        if (CollectionUtils.isNotEmpty(traveler.getDnBMatchContexts())) {
            DnBMatchContext matchContext = traveler.getDnBMatchContexts().get(0);
            if (matchContext != null) {
                matchRecord.setDnbCode(matchContext.getDnbCode());
            }
        }
    }

    private void setDebugValues(MatchTraveler traveler, InternalOutputRecord matchRecord) {
        if (traveler.getMatchInput().isMatchDebugEnabled()) {
            List<String> debugValues = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(traveler.getDnBMatchContexts())) {
                DnBMatchContext matchContext = traveler.getDnBMatchContexts().get(0);
                if (matchContext != null) {
                    String duns = matchContext.getDuns() != null ? matchContext.getDuns() : "";
                    debugValues.add(duns);
                    String value = matchContext.getOrigDuns() != null ? matchContext.getOrigDuns() : duns;
                    debugValues.add(value);
                    value = matchContext.getConfidenceCode() != null ? matchContext.getConfidenceCode() + "" : "";
                    debugValues.add(value);
                    value = matchContext.getMatchGrade() != null && matchContext.getMatchGrade().getRawCode() != null
                            ? matchContext.getMatchGrade().getRawCode() : "";
                    debugValues.add(value);
                    value = matchContext.getHitWhiteCache() != null ? matchContext.getHitWhiteCache() + "" : "";
                    debugValues.add(value);
                    value = matchContext.isPassAcceptanceCriteria() ? matchContext.isPassAcceptanceCriteria() + "" : "";
                    debugValues.add(value);
                    addNameLocationValues(debugValues, matchContext);
                    matchRecord.setDebugValues(debugValues);
                }
            }
        }
    }

    private void addNameLocationValues(List<String> debugValues, DnBMatchContext matchContext) {
        debugValues.add(getFieldValue(matchContext, "name"));
        debugValues.add(getFieldValue(matchContext, "street"));
        debugValues.add(getFieldValue(matchContext, "city"));
        debugValues.add(getFieldValue(matchContext, "state"));
        debugValues.add(getFieldValue(matchContext, "countryCode"));
        debugValues.add(getFieldValue(matchContext, "zipcode"));
        debugValues.add(getFieldValue(matchContext, "phoneNumber"));
    }

    private String getFieldValue(DnBMatchContext matchContext, String field) {
        if (matchContext.getMatchedNameLocation() == null) {
            return "";
        }
        try {
            Object value = BeanUtils.getProperty(matchContext.getMatchedNameLocation(), field);
            String valueStr = value != null ? value.toString() : "";
            return valueStr;
        } catch (Exception ex) {
            log.warn("Failed to get the value for field=" + field);
            return "";
        }
    }

    private Level setLogLevel(Level logLevel) {
        if (logLevel == null) {
            logLevel = Level.INFO;
        }
        return logLevel;
    }

    @MatchStep
    private <T extends OutputRecord> List<Future<Object>> callMatchInternal(List<T> matchRecords,
            MatchInput matchInput) {

        List<Future<Object>> matchFutures = new ArrayList<>();
        for (T record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            if (StringUtils.isNotEmpty(matchRecord.getLatticeAccountId())
                    || StringUtils.isNotEmpty(matchRecord.getCdlId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                MatchTraveler travelContext = new MatchTraveler(matchInput.getRootOperationUid(), matchKeyTuple);
                matchRecord.setTravelerId(travelContext.getTravelerId());
                travelContext.setMatchInput(matchInput);
                travelContext.setTravelTimeout(actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT);
                matchFutures.add(askMatchAnchor(travelContext));

                // send to collector
                String domain = matchKeyTuple.getDomain();
                if (StringUtils.isNotBlank(domain)) {
                    domainCollectService.enqueue(domain);
                }
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

    private Future<Object> askMatchAnchor(MatchTraveler traveler) {
        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        try {
            return actorSystem.askAnchor(traveler, timeout);
        } catch (ExecutionException e) {
            throw new RuntimeException("Fail to ask match anchor");
        }
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
                            + StringEscapeUtils.escapeJson(ExceptionUtils.getStackTrace(logEntry.getThrowable())));
                    record.addErrorMessages(logEntry.getMessage() + " : " + logEntry.getThrowable().getMessage());
                }
            }
            if (logEntry.getLevel().isGreaterOrEqual(Level.ERROR)) {
                if (logEntry.getThrowable() == null) {
                    record.addErrorMessages(logEntry.getMessage());
                } else {
                    record.addErrorMessages(logEntry.getMessage() + " : " + logEntry.getThrowable().getMessage());
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
