package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.TravelLog;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
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

    @Inject
    private MatchActorSystem actorSystem;

    @Value("${datacloud.match.publish.match.history:false}")
    private boolean isMatchHistoryEnabled;

    @Inject
    private DomainCollectService domainCollectService;

    @Lazy
    @Inject
    private EntityMatchMetricService entityMatchMetricService;

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
                // TODO: For transaction match, there are multiple entity id to
                // fetch: traveler.getEntityIds()
                if (OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
                    matchRecord.setEntityId(result);
                    if (MapUtils.isNotEmpty(traveler.getNewEntityIds())) {
                        // copy new entity IDs map
                        // $JAW$ Why might there be more than one new Entity ID?
                        matchRecord.setNewEntityIds(traveler.getNewEntityIds());
                    }
                    matchRecord.setEntityIds(traveler.getEntityIds());
                    // Copy data from EntityMatchKeyRecord in MatchTraveler that was set by MatchPlannerMicroEngineActor
                    // into the InternalOutputRecord.
                    copyFromEntityToInternalOutputRecord(traveler.getEntityMatchKeyRecord(), matchRecord);

                    // $JAW$ Need copy information from MatchTraveler to a place where we can add it to
                    // EntityMatchHistory.
                    matchRecord.setEntityMatchHistory(generateEntityMatchHistory(traveler));

                } else {
                    matchRecord.setLatticeAccountId(result);
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
                FuzzyMatchHistory history = new FuzzyMatchHistory(traveler);
                fuzzyMatchHistories.add(history);
                if (OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
                    entityMatchMetricService.recordMatchHistory(history);
                }
                if (isMatchHistoryEnabled)
                    matchRecord.setFabricMatchHistory(getDnbMatchHistory(matchRecord, traveler));
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
                dumpEntityMatchErrors(matchRecord, traveler);
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
            // TODO(jwinter): Need to make sure InternalOutputRecord works for Entity Match.
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            if (StringUtils.isNotEmpty(matchRecord.getLatticeAccountId())
                    || StringUtils.isNotEmpty(matchRecord.getEntityId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                // For now, pass in a null MatchKeyTuple for Entity Match since this will be handled by the first Actor
                // which is a Match Planner actor.
                MatchTraveler matchTraveler = null;
                if (OperationalMode.ENTITY_MATCH.equals(matchInput.getOperationalMode())) {
                    matchTraveler = new MatchTraveler(matchInput.getRootOperationUid(), null);
                    matchTraveler.setInputDataRecord(matchRecord.getInput());
                    matchTraveler.setEntityKeyPositionMaps(matchRecord.getEntityKeyPositionMaps());
                    EntityMatchKeyRecord entityMatchKeyRecord = new EntityMatchKeyRecord();
                    entityMatchKeyRecord.setOrigTenant(matchRecord.getOrigTenant());
                    entityMatchKeyRecord.setParsedTenant(matchRecord.getParsedTenant());
                    matchTraveler.setEntityMatchKeyRecord(entityMatchKeyRecord);
                    // 1st decision graph's entity is just final target entity
                    matchTraveler.setEntity(matchInput.getTargetEntity());
                } else {
                    MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                    matchTraveler = new MatchTraveler(matchInput.getRootOperationUid(), matchKeyTuple);
                    String domain = matchKeyTuple.getDomain();
                    if (StringUtils.isNotBlank(domain)) {
                        domainCollectService.enqueue(domain);
                    }
                }
                matchTraveler.setMatchInput(matchInput);
                matchRecord.setTravelerId(matchTraveler.getTravelerId());
                matchTraveler.setTravelTimeout(actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT);
                matchFutures.add(askMatchAnchor(matchTraveler));
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
        return actorSystem.askAnchor(traveler, timeout);
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

    // Used only for Entity Match to copy data from the smaller EntityMatchKeyRecord passed along in the MatchTraveler to
    // the InternalOutputRecord used outside the actor system.
    void copyFromEntityToInternalOutputRecord(EntityMatchKeyRecord entityRecord, InternalOutputRecord internalRecord) {
        internalRecord.setParsedDomain(entityRecord.getParsedDomain());
        internalRecord.setPublicDomain(entityRecord.isPublicDomain());
        internalRecord.setMatchEvenIsPublicDomain(entityRecord.isMatchEvenIsPublicDomain());
        internalRecord.setParsedDuns(entityRecord.getParsedDuns());
        internalRecord.setParsedNameLocation(entityRecord.getParsedNameLocation());
        internalRecord.setParsedEmail(entityRecord.getParsedEmail());
        internalRecord.setParsedTenant(entityRecord.getParsedTenant());
        internalRecord.setParsedSystemIds(entityRecord.getParsedSystemIds());
        internalRecord.setOrigDomain(entityRecord.getOrigDomain());
        internalRecord.setOrigNameLocation(entityRecord.getOrigNameLocation());
        internalRecord.setOrigDuns(entityRecord.getOrigDuns());
        internalRecord.setOrigEmail(entityRecord.getOrigEmail());
        internalRecord.setOrigTenant(entityRecord.getOrigTenant());
        internalRecord.setFailed(entityRecord.isFailed());
        internalRecord.setErrorMessages(entityRecord.getErrorMessages());
        internalRecord.setOrigSystemIds(entityRecord.getOrigSystemIds());
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

    /*
     * traveler#getMatchInput should not be null
     */
    private void dumpEntityMatchErrors(@NotNull InternalOutputRecord record, @NotNull MatchTraveler traveler) {
        if (!OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
            return;
        }
        if (CollectionUtils.isNotEmpty(traveler.getEntityMatchErrors())) {
            traveler.getEntityMatchErrors().forEach(record::addErrorMessages);
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

    EntityMatchHistory generateEntityMatchHistory(MatchTraveler traveler) {
        EntityMatchHistory history = new EntityMatchHistory();

        history.setBusinessEntity(traveler.getMatchInput().getTargetEntity());
        //history.setEntityId((String) traveler.getResult());
        if (StringUtils.isBlank(history.getBusinessEntity())) {
            log.error("$JAW$ Found null or blank BusinessEntity for MatchTraveler");
            return null;
        } else if (MapUtils.isEmpty(traveler.getEntityIds())) {
            log.error("$JAW$ Found null or empty Entity Match Result Map for MatchTraveler");
            return null;
        }
        history.setEntityId(traveler.getEntityIds().get(history.getBusinessEntity()));
        if (MapUtils.isEmpty(traveler.getEntityMatchKeyTuples())) {
            log.error("$JAW$ Found null or empty Entity MatchKeyTuples Map in MatchTraveler");
            return null;
        } else {
            log.error("$JAW$ EntityMatchKeyTuples is: ");
            for (Map.Entry<String, MatchKeyTuple> entry : traveler.getEntityMatchKeyTuples().entrySet())  {
                log.error("$JAW$    Entity: " + entry.getKey());
                log.error("$JAW$    MatchKeyTuple: " + entry.getValue().toString());
            }
            MatchKeyTuple matchKeyTuple = traveler.getEntityMatchKeyTuple(history.getBusinessEntity());
            if (CollectionUtils.isNotEmpty(matchKeyTuple.getSystemIds())) {
                history.setUserId(matchKeyTuple.getSystemIds().get(0).getValue());
            }
        }

        if (MapUtils.isEmpty(traveler.getEntityIds())) {
            log.error("$JAW$ Found null or empty EntityIds Map in MatchTraveler");
        } else {
            log.error("$JAW$ EntityIds is: ");
            for (Map.Entry<String, String> entry : traveler.getEntityIds().entrySet())  {
                log.error("$JAW$    Entity: " + entry.getKey());
                log.error("$JAW$    EntityId: " + entry.getValue());
            }
        }

        if (MapUtils.isEmpty(traveler.getNewEntityIds())) {
            log.error("$JAW$ Found null or empty NewEntityIds Map in MatchTraveler");
        } else {
            log.error("$JAW$ NewEntityIds is: ");
            for (Map.Entry<String, String> entry : traveler.getNewEntityIds().entrySet())  {
                log.error("$JAW$    Entity: " + entry.getKey());
                log.error("$JAW$    NewEntityId: " + entry.getValue());
            }
        }

        if (CollectionUtils.isEmpty(traveler.getEntityMatchLookupResults())) {
            log.error("$JAW$ Found null or empty EntityMatchLookupResults List in MatchTraveler");
            return null;
        }
        boolean foundResult = false;
        log.error("$JAW$ Iterate through Match Results:");
        for (Pair<MatchKeyTuple, List<String>> pair : traveler.getEntityMatchLookupResults()) {
            if (pair.getKey() == null) {
                log.error("$JAW$   MatchKeyTuple: null");
            } else {
                log.error("$JAW$   MatchKeyTuple: " + pair.getKey().toString());
            }

            String resultList = "";
            for (String result : pair.getValue()) {
                if (result != null) {
                    if (!foundResult) {
                        history.setMatchKeyTuple(pair.getKey());
                        foundResult = true;
                        resultList += ">>> " + result + " <<< ";
                    } else {
                        resultList += result + " ";
                    }
                } else {
                    resultList += "null ";
                }
            }

            log.error(":$JAW$   Results: " + resultList);

            // Check if there is a non-null result for this MatchKeyTuple.
            /*
            for (String result : pair.getValue()) {
                if (result != null) {
                    history.setMatchKeyTuple(pair.getKey());
                    break;
                }
            }
            */
        }

        log.error("$JAW$ EntityMatchHistory:\n" + "   Entity: " + history.getBusinessEntity() + "\n   Entity ID: "
                + history.getEntityId() + "\n   User Provided ID: " + history.getUserId());
        if (history.getMatchKeyTuple() == null) {
            log.error("$JAW$ MatchKeyTuple: null");
        } else {
            log.error("$JAW$ MatchKeyTuple: " + history.getMatchKeyTuple().toString());
        }

        return history;
    }
}
