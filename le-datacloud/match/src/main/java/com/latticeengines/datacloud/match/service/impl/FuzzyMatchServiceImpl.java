package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ENTITY_ANONYMOUS_ID;

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
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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
                        // TODO(slin): Why might there be more than one new Entity ID?
                        matchRecord.setNewEntityIds(traveler.getNewEntityIds());
                    }
                    matchRecord.setEntityIds(traveler.getEntityIds());
                    // Copy data from EntityMatchKeyRecord in MatchTraveler that was set by MatchPlannerMicroEngineActor
                    // into the InternalOutputRecord.
                    copyFromEntityToInternalOutputRecord(traveler.getEntityMatchKeyRecord(), matchRecord);

                    // Need to copy information from MatchTraveler to a place where we can add it to MatchHistory.
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
                if (isMatchHistoryEnabled) {
                    matchRecord.setFabricMatchHistory(getDnbMatchHistory(matchRecord, traveler));
                }
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

    private EntityMatchHistory generateEntityMatchHistory(MatchTraveler traveler) {
        EntityMatchHistory history = new EntityMatchHistory();

        log.debug("------------------------ Entity Match History Debug Logs ------------------------");

        // Extract Business Entity
        if (StringUtils.isBlank(traveler.getMatchInput().getTargetEntity())) {
            log.error("Found null or blank BusinessEntity in MatchTraveler");
            return null;
        }
        history.setBusinessEntity(traveler.getMatchInput().getTargetEntity());
        log.debug("Business Entity: " + history.getBusinessEntity());

        // Check if DUNS was a provided MatchKey and extract Input MatchKeys
        Boolean inputHasDuns = checkDunsAndPrintInputMatchKeys(traveler, history.getBusinessEntity());
        if (inputHasDuns == null) {
            return null;
        }

        // Extract the matched Entity ID and whether there was a match.
        history.setEntityId(extractEntityId(traveler, history.getBusinessEntity()));
        history.setEntityMatched(extractMatchedState(traveler, history.getBusinessEntity(), history.getEntityId()));

        // Get Full MatchKeyTuple for Business Entity.
        history.setFullMatchKeyTuple(extractFullMatchKeyTuple(traveler, history.getBusinessEntity()));
        if (history.getFullMatchKeyTuple() == null) {
            return null;
        }

        // Get Customer Entity Id, if provided.
        history.setCustomerEntityId(extractCustomerEntityId(history.getFullMatchKeyTuple(),
                history.getBusinessEntity()));

        // Get MatchKeyTuple that found Entity ID.
        if (!checkEntityMatchLookupResults(traveler, history.getBusinessEntity())) {
            return null;
        }
        List<String> lookupResultList = new ArrayList<>();
        history.setMatchedMatchKeyTuple(extractMatchedMatchKeyTuple(traveler, history.getBusinessEntity(),
                lookupResultList));

        // Generate Match Type Enum describing the match.
        history.setMatchType(extractEntityMatchType(history.getBusinessEntity(), history.getMatchedMatchKeyTuple(),
                lookupResultList, inputHasDuns));
        if (history.getMatchType() == null) {
            return null;
        } else if (EntityMatchType.LDC_MATCH.name().equals(history.getMatchType())) {
            Pair<EntityMatchType, MatchKeyTuple> typeTuplePair = extractLdcMatchTypeAndTuple(traveler);
            if (typeTuplePair == null) {
                return null;
            }
            history.setMatchType(typeTuplePair.getLeft());
            history.setMatchedMatchKeyTuple(typeTuplePair.getRight());
        }

        // Add LeadToAccount Matching Results for Contacts.
        if (BusinessEntity.Contact.name().equals(history.getBusinessEntity())) {
            String accountEntity = BusinessEntity.Account.name();
            log.debug("+++ LeadToAccount Account Match Data +++");

            // Check if DUNS was a provided MatchKey and extract Input MatchKeys
            inputHasDuns = checkDunsAndPrintInputMatchKeys(traveler, accountEntity);
            if (inputHasDuns == null) {
                return null;
            }

            // Extract the matched Entity ID and whether there was a match.
            history.setL2aEntityId(extractEntityId(traveler, accountEntity));
            history.setL2aEntityMatched(extractMatchedState(traveler, accountEntity, history.getL2aEntityId()));

            // Get Full MatchKeyTuple for Business Entity.
            history.setL2aFullMatchKeyTuple(extractFullMatchKeyTuple(traveler, accountEntity));
            if (history.getL2aFullMatchKeyTuple() == null) {
                return null;
            }

            // Get Customer Entity Id, if provided.
            history.setL2aCustomerEntityId(extractCustomerEntityId(history.getL2aFullMatchKeyTuple(), accountEntity));

            // Get MatchKeyTuple that found Entity ID.
            if (!checkEntityMatchLookupResults(traveler, accountEntity)) {
                return null;
            }
            lookupResultList = new ArrayList<>();
            history.setL2aMatchedMatchKeyTuple(extractMatchedMatchKeyTuple(traveler, accountEntity, lookupResultList));

            // Generate Match Type Enum describing the match.
            history.setL2aMatchType(extractEntityMatchType(accountEntity, history.getL2aMatchedMatchKeyTuple(),
                    lookupResultList, inputHasDuns));
            if (history.getL2aMatchType() == null) {
                return null;
            } else if (EntityMatchType.LDC_MATCH.name().equals(history.getL2aMatchType())) {
                Pair<EntityMatchType, MatchKeyTuple> typeTuplePair = extractLdcMatchTypeAndTuple(traveler);
                if (typeTuplePair == null) {
                    return null;
                }
                history.setL2aMatchType(typeTuplePair.getLeft());
                history.setL2aMatchedMatchKeyTuple(typeTuplePair.getRight());
            }
        }

        // Log extra debug information about the match.
        generateEntityMatchHistoryDebugLogs(traveler);

        log.debug("------------------------ END Entity Match History Debug Logs ------------------------");

        return history;
    }

    // Determine if the input MatchKeys included DUNS (which is important for classifying the match type later on).
    // Print out the Input MatchKey key and value pairs for debugging.
    private Boolean checkDunsAndPrintInputMatchKeys(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityKeyPositionMaps())) {
            log.error("Found null or empty EntityKeyPositionMaps in MatchTraveler");
            return null;
        }
        if (!traveler.getEntityKeyPositionMaps().containsKey(entity)) {
            log.error("EntityKeyPositionMaps missing entry for Business Entity: " + entity);
            return null;
        }
        if (CollectionUtils.isEmpty(traveler.getInputDataRecord())) {
            log.error("InputDataRecord list null or empty in MatchTraveler");
            return null;
        }
        boolean inputHasDuns = false;
        Map<MatchKey, List<Integer>> matchKeyPosMap = traveler.getEntityKeyPositionMaps().get(entity);
        StringBuilder columnKeys = new StringBuilder();
        StringBuilder columnValues = new StringBuilder();
        for (Map.Entry<MatchKey, List<Integer>> entry : matchKeyPosMap.entrySet()) {
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                for (Integer pos : entry.getValue()) {
                    columnKeys.append(String.format("%30s", entry.getKey().name()));
                    columnValues.append(String.format("%30s", traveler.getInputDataRecord().get(pos)));

                    if (MatchKey.DUNS.equals(entry.getKey())
                            && StringUtils.isNotBlank((String) traveler.getInputDataRecord().get(pos))) {
                        inputHasDuns = true;
                    }
                }
            }
        }
        log.debug("Input MatchKeys:\n" + columnKeys + "\n" + columnValues);
        return inputHasDuns;
    }

    // Extract the matched entity ID for a given entity.
    private String extractEntityId(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityIds())) {
            log.error("Found null or empty EntityIds Map in MatchTraveler");
            return null;
        }
        if (!traveler.getEntityIds().containsKey(entity)) {
            log.error("EntityIds missing entry for Business Entity: " + entity);
            return null;
        }
        return traveler.getEntityIds().get(entity);
    }

    // Extract the matched state of this record.
    private String extractMatchedState(MatchTraveler traveler, String entity, String entityId) {
        String matched;
        if (ENTITY_ANONYMOUS_ID.equals(entityId)) {
            matched = "ANONYMOUS";
        } else if (entityId == null || (MapUtils.isNotEmpty(traveler.getNewEntityIds())
                && traveler.getNewEntityIds().containsKey(entity))) {
            matched = "NO MATCH";
        } else {
            matched = "MATCHED";
        }
        log.debug("Matched: " + matched);
        log.debug("Entity Id: " + entityId);
        return matched;
    }

    // Extract Full MatchKeyTuple for the given Business Entity.
    private MatchKeyTuple extractFullMatchKeyTuple(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityMatchKeyTuples())) {
            log.error("Found null or empty EntityMatchKeyTuples Map in MatchTraveler");
            return null;
        }
        if (!traveler.getEntityMatchKeyTuples().containsKey(entity)) {
            log.error("EntityMatchKeyTuples missing entry for Business Entity: " + entity);
            return null;
        }
        if (traveler.getEntityMatchKeyTuples().get(entity) == null) {
            log.error("EntityMatchKeyTuples has null entry for Business Entity: " + entity);
            return null;
        }
        log.debug("Full MatchKeyTuple: " + traveler.getEntityMatchKeyTuple(entity).toString());
        return traveler.getEntityMatchKeyTuple(entity);
    }

    // Extract the customer provided entity ID (ie. CustomerAccountId or CustomerContactId).
    private String extractCustomerEntityId(MatchKeyTuple tuple, String entity) {
        String customerEntityId = null;
        if (tuple != null && CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
            for (Pair<String, String> systemId : tuple.getSystemIds()) {
                if (systemId.getKey().equals(InterfaceName.CustomerAccountId.name())
                        && StringUtils.isNotBlank(systemId.getValue())
                        && BusinessEntity.Account.name().equals(entity)) {
                    customerEntityId = systemId.getValue();
                    break;
                } else if (systemId.getKey().equals(InterfaceName.CustomerContactId.name())
                        && StringUtils.isNotBlank(systemId.getValue())
                        && BusinessEntity.Contact.name().equals(entity)) {
                    customerEntityId = systemId.getValue();
                    break;
                }
            }
        }
        log.debug("Customer Entity ID: " + customerEntityId);
        return customerEntityId;
    }

    // Make sure the EntityMatchLookupResults data structure is valid.
    private boolean checkEntityMatchLookupResults(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityMatchLookupResults())) {
            log.error("Found null or empty EntityMatchLookupResults Map in MatchTraveler");
            return false;
        }
        if (!traveler.getEntityMatchLookupResults().containsKey(entity)) {
            log.error("EntityMatchLookupResults missing entry for Business Entity: " + entity);
            return false;
        }
        if (traveler.getEntityMatchLookupResults().get(entity) == null) {
            log.error("EntityMatchLookupResults has null entry for Business Entity: " + entity);
            return false;
        }
        return true;
    }

    // Extract MatchKeyTuple used in the successful match for the given Business Entity.
    private MatchKeyTuple extractMatchedMatchKeyTuple(MatchTraveler traveler, String entity,
                                                       List<String> lookupResultList) {
        for (Pair<MatchKeyTuple, List<String>> pair : traveler.getEntityMatchLookupResult(entity)) {
            for (String result : pair.getValue()) {
                if (result != null) {
                    lookupResultList.addAll(pair.getValue());
                    if (pair.getKey() == null) {
                        log.error("MatchedMatchKeyTuple has value but null key for Business Entity: " + entity);
                        return null;
                    }
                    return pair.getKey();
                }
            }
        }
        return null;
    }

    // Extract MatchType Enum describing match.
    private EntityMatchType extractEntityMatchType(
            String entity, MatchKeyTuple tuple, List<String> lookupResultList, boolean inputHasDuns) {
        EntityMatchType type;

        if (tuple == null) {
            type = EntityMatchType.NO_MATCH;
        } else {
            type = EntityMatchType.UNKNOWN;

            boolean hasAccountId = false;
            if (CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
                if (lookupResultList.size() != tuple.getSystemIds().size()) {
                    log.error("EntityMatchLookupResults results and MatchKeyTuple SystemIds sizes don't match");
                    log.error("EntityMatchLookupResults: " + lookupResultList.toString());
                    log.error("MatchKeyTuple SystemIds: " + tuple.getSystemIds().toString());
                    return null;
                }
                int i = 0;
                for (Pair<String, String> systemId : tuple.getSystemIds()) {
                    if (systemId.getKey().equals(InterfaceName.CustomerAccountId.name())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Account.name().equals(entity)) {
                        type = EntityMatchType.ACCOUNTID;
                        //log.debug("MatchKeyTuple contains CustomerAccountId: " + systemId.getValue());
                        break;
                    } else if (systemId.getKey().equals(InterfaceName.CustomerContactId.name())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Contact.name().equals(entity)) {
                        type = EntityMatchType.CONTACTID;
                        //log.debug("MatchKeyTuple contains CustomerContactId: " + systemId.getValue());
                        break;
                    } else if (systemId.getKey().equals(InterfaceName.AccountId.name())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && BusinessEntity.Contact.name().equals(entity)) {
                        //log.debug("MatchKeyTuple contains AccountId: " + systemId.getValue());
                        hasAccountId = true;
                    } else if (StringUtils.isNotBlank(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))) {
                        type = EntityMatchType.SYSTEMID;
                        //log.debug("MatchKeyTuple contains SystemId: " + systemId.getKey() + " with value: "
                        //        + systemId.getValue());
                        break;
                    }
                    i++;
                }
            }

            if (type == EntityMatchType.UNKNOWN) {
                if (tuple.hasDomain()) {
                    type = EntityMatchType.DOMAIN_COUNTRY;
                } else if (tuple.hasDuns()) {
                    // Need to figure out LDC Case
                    if (inputHasDuns) {
                        type = EntityMatchType.DUNS;
                    } else {
                        type = EntityMatchType.LDC_MATCH;
                    }
                } else if (tuple.hasEmail()) {
                    if (hasAccountId) {
                        type = EntityMatchType.EMAIL_ACCOUNTID;
                    } else {
                        type = EntityMatchType.EMAIL;
                    }
                } else if (tuple.hasName()) {
                    if (BusinessEntity.Account.name().equals(entity)) {
                        type = EntityMatchType.NAME_COUNTRY;
                    } else if (tuple.hasPhoneNumber()) {
                        if (hasAccountId) {
                            type = EntityMatchType.NAME_PHONE_ACCOUNTID;
                        } else {
                            type = EntityMatchType.NAME_PHONE;
                        }
                    }
                }
            }
        }

        // Log the match type and matched MatchKeyTuple here for non-LDC matches.  LDC matches require additional
        // processing.
        if (!EntityMatchType.LDC_MATCH.equals(type)) {
            log.debug("MatchType is: " + type);
            log.debug("MatchedMatchKeyTuple: " + tuple);
        } else if (!BusinessEntity.Account.name().equals(entity)) {
            log.error("Found LDC Match type for entity " + entity + " which should not be possible");
            return null;
        }
        return type;
    }

    private Pair<EntityMatchType, MatchKeyTuple> extractLdcMatchTypeAndTuple(MatchTraveler traveler) {
        if (CollectionUtils.isEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
            log.error("MatchType is LDC_MATCH but Type to Tuple list is null or empty");
            return null;
        }

        List<Pair<MatchKeyTuple, List<String>>> ldcMatchLookupResultList = traveler.getEntityMatchLookupResult(
                BusinessEntity.LatticeAccount.name());
        if (CollectionUtils.isEmpty(ldcMatchLookupResultList)) {
            log.error("MatchType is LDC_MATCH but LDC Match Lookup Results is null or empty");
            return null;
        }

        // Iterate through the lists of LDC Match Lookup Results and LDC Match Type / MatchKeyTuple pairs, to find the
        // the first successful result.  The record the corresponding LDC Match Type and MatchKeyTuple of that result.
        if (ldcMatchLookupResultList.size() != traveler.getEntityLdcMatchTypeToTupleList().size()) {
            log.error("EntityMatchLookupResult for " + BusinessEntity.LatticeAccount.name()
                    + " and EntityLdcMatchTypeToTupleList are not the same length: "
                    + ldcMatchLookupResultList.size() + " vs " + traveler.getEntityLdcMatchTypeToTupleList().size());
        }

        EntityMatchType type = null;
        MatchKeyTuple tuple = null;
        int i;
        boolean foundResult = false;
        for (i = 0; i < ldcMatchLookupResultList.size() && !foundResult; i++) {
            if (CollectionUtils.isEmpty(ldcMatchLookupResultList.get(i).getValue())) {
                log.error("EntityMatchLookupResult for " + BusinessEntity.LatticeAccount.name()
                        + " has list entry composed of a Pair with null value and key MatchKeyTuple: "
                        + ldcMatchLookupResultList.get(i).getKey());
                return null;
            }
            for (String result : ldcMatchLookupResultList.get(i).getValue()) {
                if (result != null) {
                    type = traveler.getEntityLdcMatchTypeToTupleList().get(i).getLeft();
                    tuple = traveler.getEntityLdcMatchTypeToTupleList().get(i).getRight();
                    foundResult = true;
                    break;
                }
            }
        }

        if (!foundResult) {
            log.error("MatchType is LDC_MATCH but LDC Match Lookup Results has no entry with non-null result");
            return null;
        } else if (type == null) {
            log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null EntityMatchType");
            return null;
        } else if (tuple == null) {
            log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null MatchKeyTuple for type: " + type);
            return null;
        }

        // If the MatchKeyTuple has a DUNS fields and the EntityMatchType is not LDC DUNS or DUNS plus Domain, then the
        // LDC Match has stuck a DUNS value in the matched MatchKeyTuple that wasn't actually part of the input match
        // tuple used for matching.  In this case, a copy of the MatchKeyTuple without the DUNS field needs to be
        // created and returned.
        if (tuple.hasDuns() &&
                !EntityMatchType.LDC_DUNS.equals(type) && !EntityMatchType.LDC_DUNS_DOMAIN.equals(type)) {
            MatchKeyTuple fixedTuple = new MatchKeyTuple();
            fixedTuple.setDomain(tuple.getDomain());
            fixedTuple.setName(tuple.getName());
            fixedTuple.setCity(tuple.getCity());
            fixedTuple.setState(tuple.getState());
            fixedTuple.setCountry(tuple.getCountry());
            fixedTuple.setCountryCode(tuple.getCountryCode());
            fixedTuple.setZipcode(tuple.getZipcode());
            fixedTuple.setPhoneNumber(tuple.getPhoneNumber());
            fixedTuple.setEmail(tuple.getEmail());
            // Don't set System ID since it is not used in LDC match even if it is set.
            tuple = fixedTuple;
        }
        log.debug("MatchType is: " + type);
        log.debug("MatchedMatchKeyTuple: " + tuple);

        return Pair.of(type, tuple);
    }

    // Assumes traveler.getEntityIds(), traveler.getEntityMatchKeyTuples(), and traveler.getEntityMatchLookupResults()
    // do not return null because they were checked by generateEntityMatchHistory().
    private void generateEntityMatchHistoryDebugLogs(MatchTraveler traveler) {
        log.debug("------------------------ Entity Match History Extra Debug Logs ------------------------");
        log.debug("EntityIds are: ");
        for (Map.Entry<String, String> entry : traveler.getEntityIds().entrySet())  {
            log.debug("   Entity: " + entry.getKey());
            log.debug("   EntityId: " + entry.getValue());
        }

        if (MapUtils.isEmpty(traveler.getNewEntityIds())) {
            log.debug("Found null or empty NewEntityIds Map in MatchTraveler");
        } else {
            log.debug("NewEntityIds is: ");
            for (Map.Entry<String, String> entry : traveler.getNewEntityIds().entrySet())  {
                log.debug("    Entity: " + entry.getKey());
                log.debug("    NewEntityId: " + entry.getValue());
            }
        }

        log.debug("EntityMatchKeyTuples is: ");
        for (Map.Entry<String, MatchKeyTuple> entry : traveler.getEntityMatchKeyTuples().entrySet())  {
            log.debug("    Entity: " + entry.getKey());
            log.debug("    MatchKeyTuple: " + entry.getValue().toString());
        }

        log.debug("Iterate through EntityMatchLookupResults:");
        for (Map.Entry<String, List<Pair<MatchKeyTuple, List<String>>>> entry :
                traveler.getEntityMatchLookupResults().entrySet()) {
            boolean foundResult = false;
            log.debug("   MatchKeyTuple Lookup Results for " + entry.getKey());
            for (Pair<MatchKeyTuple, List<String>> pair : entry.getValue()) {
                if (pair.getKey() == null) {
                    log.debug("    MatchKeyTuple: null");
                } else {
                    log.debug("    MatchKeyTuple: " + pair.getKey().toString());
                }

                String resultList = "";
                for (String result : pair.getValue()) {
                    if (result != null) {
                        if (!foundResult) {
                            foundResult = true;
                            resultList += ">>> " + result + " <<< ";
                        } else {
                            resultList += result + " ";
                        }
                    } else {
                        resultList += "null ";
                    }
                }

                log.debug("    Results: " + resultList);
            }
        }

        if (CollectionUtils.isNotEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
            log.debug("Iterate through EntityLdcMatchTypeToTupleList:");
            for (Pair<EntityMatchType, MatchKeyTuple> pair : traveler.getEntityLdcMatchTypeToTupleList()) {
                log.debug("    MatchType: " + pair.getLeft() + "  MatchKeyTuple: " + pair.getRight());
            }
        } else {
            log.debug("EntityLdcMatchTypeToTupleList is empty");
        }

        //log.debug("------------------------ END Entity Match History Extra Debug Logs ------------------------");
    }

}
