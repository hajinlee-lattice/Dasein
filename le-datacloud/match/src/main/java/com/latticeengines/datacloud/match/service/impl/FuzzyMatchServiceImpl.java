package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
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
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class FuzzyMatchServiceImpl implements FuzzyMatchService {
    private static final String FABRIC_MATCH_HISTORY = "FabricMatchHistory";

    private static final Log log = LogFactory.getLog(FuzzyMatchServiceImpl.class);

    private static final Timeout REALTIME_TIMEOUT = new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
    private static final Timeout BATCH_TIMEOUT = new Timeout(new FiniteDuration(3, TimeUnit.HOURS));

    @Resource(name = "genericFabricEntityManager")
    private GenericFabricEntityManager<MatchHistory> fabricEntityManager;

    @Autowired
    private MatchActorSystem actorSystem;

    @Value("${datacloud.match.publish.match.history:false}")
    private boolean isMatchHistoryEnabled;

    @Autowired
    private DomainCollectService domainCollectService;

    @PostConstruct
    public void init() {
        if (isMatchHistoryEnabled) {
            fabricEntityManager.createOrGetNamedBatchId(FABRIC_MATCH_HISTORY, null, false);
        }
    }

    @Override
    public <T extends OutputRecord> void callMatch(List<T> matchRecords, MatchInput matchInput) throws Exception {
        checkRecordType(matchRecords);
        Level logLevel = setLogLevel(matchInput.getLogLevel());
        matchInput.setLogLevel(logLevel);
        List<Future<Object>> matchFutures = callMatchInternal(matchRecords, matchInput);
        fetchIdResult(matchRecords, logLevel, matchFutures);
    }

    @Override
    public <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, MatchInput matchInput)
            throws Exception {
        Level logLevel = setLogLevel(matchInput.getLogLevel());
        matchInput.setLogLevel(logLevel);
        return callMatchInternal(matchRecords, matchInput);
    }

    @Override
    public <T extends OutputRecord> void fetchIdResult(List<T> matchRecords, Level logLevel,
            List<Future<Object>> matchFutures) throws Exception {
        logLevel = setLogLevel(logLevel);
        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        List<FuzzyMatchHistory> fuzzyMatchHistories = new ArrayList<>();
        List<MatchHistory> matchHistories = new ArrayList<>();
        for (int idx = 0; idx < matchFutures.size(); idx++) {
            Future<Object> future = matchFutures.get(idx);
            if (future != null) {
                // null future means already has lattice account id, or failed
                // in initialization
                MatchTraveler traveler = (MatchTraveler) Await.result(future, timeout.duration());
                InternalOutputRecord matchRecord = (InternalOutputRecord) matchRecords.get(idx);
                String result = (String) traveler.getResult();
                matchRecord.setLatticeAccountId(result);
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
                    matchHistories.add(getMatchHistory(matchRecord, traveler));
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
            }
        }

        writeFuzzyMatchHistory(fuzzyMatchHistories);
        publishMatchHistory(matchHistories);
    }

    private MatchHistory getMatchHistory(InternalOutputRecord matchRecord, MatchTraveler traveler) {
        MatchHistory matchHistory = new MatchHistory();
        matchHistory.setDomain(matchRecord.getParsedDomain()).setDuns(matchRecord.getParsedDuns())
                .setEmail(matchRecord.getParsedEmail()).setIsPublicDomain(matchRecord.isPublicDomain());
        matchHistory.withNameLocation(matchRecord.getParsedNameLocation()).setMatched(matchRecord.isMatched())
                .setLatticeAccountId(matchRecord.getLatticeAccountId()).setMatchMode(traveler.getMode())
                .setTimestampSeconds(System.currentTimeMillis() / 1000);
        if (CollectionUtils.isNotEmpty(traveler.getDnBMatchContexts())) {
            DnBMatchContext matchContext = traveler.getDnBMatchContexts().get(0);
            if (matchContext != null) {
                matchHistory.withDnBMatchResult(matchContext);
            }
        }
        return matchHistory;
    }

    private void publishMatchHistory(List<MatchHistory> matchHistories) {
        if (!isMatchHistoryEnabled) {
            return;
        }
        if (CollectionUtils.isEmpty(matchHistories)) {
            return;
        }
        for (MatchHistory matchHistory : matchHistories) {
            GenericRecordRequest recordRequest = new GenericRecordRequest();
            recordRequest.setId(UUID.randomUUID().toString());
            matchHistory.setId(recordRequest.getId());
            recordRequest.setStores(Arrays.asList(FabricStoreEnum.HDFS))
                    .setRepositories(Arrays.asList(FABRIC_MATCH_HISTORY)).setBatchId(FABRIC_MATCH_HISTORY);
            fabricEntityManager.publishEntity(recordRequest, matchHistory, MatchHistory.class);
        }
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
                    String value = matchContext.getDuns() != null ? matchContext.getDuns() : "";
                    debugValues.add(value);
                    value = matchContext.getConfidenceCode() != null ? matchContext.getConfidenceCode() + "" : "";
                    debugValues.add(value);
                    value = matchContext.getMatchGrade() != null && matchContext.getMatchGrade().getRawCode() != null ? matchContext
                            .getMatchGrade().getRawCode() : "";
                    debugValues.add(value);
                    value = matchContext.getHitWhiteCache() != null ? matchContext.getHitWhiteCache() + "" : "";
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

    private <T extends OutputRecord> List<Future<Object>> callMatchInternal(List<T> matchRecords, MatchInput matchInput) {

        List<Future<Object>> matchFutures = new ArrayList<>();
        for (T record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            if (StringUtils.isNotEmpty(matchRecord.getLatticeAccountId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                MatchTraveler travelContext = new MatchTraveler(matchInput.getRootOperationUid(), matchKeyTuple);
                matchRecord.setTravelerId(travelContext.getTravelerId());
                travelContext.setMatchInput(matchInput);
                travelContext.setTravelTimeout(actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT);
                matchFutures.add(askFuzzyMatchAnchor(travelContext));

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
                    record.getErrorMessages().add(logEntry.getMessage() + " : " + logEntry.getThrowable().getMessage());
                }
            }
            if (logEntry.getLevel().isGreaterOrEqual(Level.ERROR)) {
                if (logEntry.getThrowable() == null) {
                    record.getErrorMessages().add(logEntry.getMessage());
                } else {
                    record.getErrorMessages().add(logEntry.getMessage() + " : " + logEntry.getThrowable().getMessage());
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
