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
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
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
                // Copy Data Cloud Version from traveler to the InternalOutputRecord.
                matchRecord.setDataCloudVersion(traveler.getDataCloudVersion());

                String result = (String) traveler.getResult();
                if (OperationalMode.isEntityMatch(traveler.getMatchInput().getOperationalMode())) {
                    populateEntityMatchRecordWithTraveler(traveler, result, matchRecord);
                } else {
                    matchRecord.setLatticeAccountId(result);
                    if (StringUtils.isNotEmpty(result)) {
                        matchRecord.setMatched(true);
                    } else {
                        matchRecord.addErrorMessages("Cannot find a match in data cloud for the input.");
                    }
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
                if (OperationalMode.isEntityMatch(traveler.getMatchInput().getOperationalMode())) {
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

    private void populateEntityMatchRecordWithTraveler(MatchTraveler traveler, String result,
                                                       InternalOutputRecord matchRecord) {
        matchRecord.setEntityId(result);
        if (MapUtils.isNotEmpty(traveler.getNewEntityIds())) {
            // copy new entity IDs map
            matchRecord.setNewEntityIds(traveler.getNewEntityIds());
        }
        matchRecord.setEntityIds(traveler.getEntityIds());
        if (MapUtils.isNotEmpty(traveler.getEntityIds()) && matchRecord.getLatticeAccountId() == null) {
            String latticeAccountId = traveler.getEntityIds().get(BusinessEntity.LatticeAccount.name());
            matchRecord.setLatticeAccountId(latticeAccountId);
        }
        matchRecord.setFieldsToClear(traveler.getFieldsToClear());
        // Copy data from EntityMatchKeyRecord in MatchTraveler that was set by
        // MatchPlannerMicroEngineActor into the InternalOutputRecord.
        copyFromEntityToInternalOutputRecord(traveler.getEntityMatchKeyRecord(), matchRecord);

        // Need to copy information from MatchTraveler to a place where we can add it to
        // MatchHistory.
        matchRecord.setEntityMatchHistory(generateEntityMatchHistory(traveler));
        if (StringUtils.isNotEmpty(result) && !DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(result)) {
            matchRecord.setMatched(true);
        }
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
                            ? matchContext.getMatchGrade().getRawCode()
                            : "";
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
                    || StringUtils.isNotEmpty(matchRecord.getEntityId()) || matchRecord.isFailed()) {
                matchFutures.add(null);
            } else {
                // For now, pass in a null MatchKeyTuple for Entity Match since this will be
                // handled by the first Actor
                // which is a Match Planner actor.
                MatchTraveler matchTraveler = null;
                if (OperationalMode.isEntityMatch(matchInput.getOperationalMode())) {
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

    // Used only for Entity Match to copy data from the smaller EntityMatchKeyRecord
    // passed along in the MatchTraveler to
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
        if (!OperationalMode.isEntityMatch(traveler.getMatchInput().getOperationalMode())) {
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
        log.debug("Generating EntityMatchHistory for Match Report.");
        log.debug("------------------------ Entity Match History Debug Logs ------------------------");

        // Extract Business Entity
        if (StringUtils.isBlank(traveler.getMatchInput().getTargetEntity())) {
            log.error("Found null or blank BusinessEntity in MatchTraveler");
            return null;
        }
        history.setBusinessEntity(traveler.getMatchInput().getTargetEntity());
        log.debug("Business Entity: " + history.getBusinessEntity());

        // Check if DUNS was a provided MatchKey and extract Input MatchKeys
        String inputDuns = getInputDunsAndPrintInputMatchKeys(traveler, history.getBusinessEntity());

        // Extract the matched Entity ID and whether there was a match.
        history.setEntityId(extractEntityId(traveler, history.getBusinessEntity()));
        history.setEntityMatched(extractMatchedState(traveler, history.getBusinessEntity(), history.getEntityId()));

        // Get Full MatchKeyTuple for Business Entity.
        history.setFullMatchKeyTuple(extractFullMatchKeyTuple(traveler, history.getBusinessEntity()));
        if (history.getFullMatchKeyTuple() == null) {
            return null;
        }

        // Get Customer Entity Id, if provided.
        history.setCustomerEntityId(
                extractCustomerEntityId(history.getFullMatchKeyTuple(), history.getBusinessEntity()));

        // Get MatchKeyTuple that found Entity ID, if a match was found.
        if (!checkEntityMatchLookupResults(traveler, history.getBusinessEntity())) {
            return null;
        }
        List<String> lookupResultList = new ArrayList<>();
        history.setMatchedEntityMatchKeyTuple(
                extractMatchedMatchKeyTuple(traveler, history.getBusinessEntity(), lookupResultList));
        // Generate EntityMatchType Enum describing the match.
        history.setEntityMatchType(extractEntityMatchType(history.getBusinessEntity(),
                history.getMatchedEntityMatchKeyTuple(), lookupResultList, inputDuns));
        if (history.getEntityMatchType() == null) {
            return null;
        }

        if (BusinessEntity.Account.name().equals(history.getBusinessEntity())) {
            // Now set the LdcMatchType and the MatchedLdcMatchKeyTuple if LDC Match succeeded.
            Pair<LdcMatchType, MatchKeyTuple> typeTuplePair = extractLdcMatchTypeAndTuple(traveler);
            if (typeTuplePair == null) {
                return null;
            }
            history.setLdcMatchType(typeTuplePair.getLeft());
            history.setMatchedLdcMatchKeyTuple(typeTuplePair.getRight());
        }

        // Generate list of all Existing Entity Lookup Keys.
        history.setExistingLookupKeyList(extractExistingLookupKeyList(traveler, history.getBusinessEntity()));

        // Add LeadToAccount Matching Results for Contacts.
        if (BusinessEntity.Contact.name().equals(history.getBusinessEntity())) {
            log.debug("------------------------ BEGIN L2A Match History Debug Logs ------------------------");
            if (!generateL2aMatchHistory(traveler, history)) {
                return null;
            }
        }

        // Log extra debug information about the match.
        generateEntityMatchHistoryDebugLogs(traveler);

        log.debug("------------------------ END Entity Match History Debug Logs ------------------------");
        return history;
    }

    // If the match was for a Contact, process the Lead to Account component which runs Account match.
    // Returns true if successful, and false if something went wrong and the EntityMatchHistory generation for this
    // match should fail.
    private boolean generateL2aMatchHistory(MatchTraveler traveler, EntityMatchHistory history) {
        String accountEntity = BusinessEntity.Account.name();
        log.debug("+++ LeadToAccount Account Match Data +++");

        // Check if DUNS was a provided MatchKey and extract Input MatchKeys
        String inputDuns = getInputDunsAndPrintInputMatchKeys(traveler, accountEntity);

        // Extract the matched Entity ID and whether there was a match.
        history.setL2aEntityId(extractEntityId(traveler, accountEntity));
        history.setL2aEntityMatched(extractMatchedState(traveler, accountEntity, history.getL2aEntityId()));

        // Get Full MatchKeyTuple for Business Entity.
        history.setL2aFullMatchKeyTuple(extractFullMatchKeyTuple(traveler, accountEntity));
        if (history.getL2aFullMatchKeyTuple() == null) {
            return false;
        }

        // Get Customer Entity Id, if provided.
        history.setL2aCustomerEntityId(extractCustomerEntityId(history.getL2aFullMatchKeyTuple(), accountEntity));

        // Get MatchKeyTuple that found Entity ID, if a match was found.
        if (!checkEntityMatchLookupResults(traveler, accountEntity)) {
            return false;
        }
        List<String> lookupResultList = new ArrayList<>();
        history.setL2aMatchedEntityMatchKeyTuple(extractMatchedMatchKeyTuple(traveler, accountEntity, lookupResultList));
        // Generate L2A EntityMatchType Enum describing the match.
        history.setL2aEntityMatchType(extractEntityMatchType(accountEntity, history.getL2aMatchedEntityMatchKeyTuple(),
                lookupResultList, inputDuns));
        if (history.getL2aEntityMatchType() == null) {
            return false;
        }

        // Now set the L2aLdcMatchType and the L2aMatchedLdcMatchKeyTuple if LDC Match succeeded.
        Pair<LdcMatchType, MatchKeyTuple> typeTuplePair = extractLdcMatchTypeAndTuple(traveler);
        if (typeTuplePair == null) {
            return false;
        }
        history.setL2aLdcMatchType(typeTuplePair.getLeft());
        history.setL2aMatchedLdcMatchKeyTuple(typeTuplePair.getRight());

        // Generate list of all Existing Entity Lookup Keys.
        history.setL2aExistingLookupKeyList(extractExistingLookupKeyList(traveler, accountEntity));
        return true;
    }

    // Determine if the input MatchKeys included DUNS (which is important for classifying the match type later on).
    // Return the DUNS value if it exists or null otherwise.
    // Print out the Input MatchKey key and value pairs for debugging.
    private String getInputDunsAndPrintInputMatchKeys(MatchTraveler traveler, String entity) {
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
        String inputDuns = null;
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
                        inputDuns = (String) traveler.getInputDataRecord().get(pos);
                        log.debug("Found input DUNS value: " + inputDuns);
                    }
                }
            }
        }
        log.debug("Input MatchKeys:\n" + columnKeys + "\n" + columnValues);
        return inputDuns;
    }

    // Extract the matched entity ID for a given entity.
    private String extractEntityId(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityIds())) {
            if (OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
                log.error("Found null or empty EntityIds Map in MatchTraveler");
            }
            return null;
        }
        if (!traveler.getEntityIds().containsKey(entity)) {
            if (OperationalMode.ENTITY_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
                log.error("EntityIds missing entry for Business Entity: " + entity);
            }
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

    // Extract the customer provided entity ID (ie. CustomerAccountId or
    // CustomerContactId).
    private String extractCustomerEntityId(MatchKeyTuple tuple, String entity) {
        String customerEntityId = null;
        if (tuple != null && CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
            for (Pair<String, String> systemId : tuple.getSystemIds()) {
                if (InterfaceName.CustomerAccountId.name().equals(systemId.getKey())
                        && StringUtils.isNotBlank(systemId.getValue())
                        && BusinessEntity.Account.name().equals(entity)) {
                    customerEntityId = systemId.getValue();
                    break;
                } else if (InterfaceName.CustomerContactId.name().equals(systemId.getKey())
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
                        log.error("MatchedEntityMatchKeyTuple has value but null key for Business Entity: " + entity);
                        return null;
                    }
                    return pair.getKey();
                }
            }
        }
        return null;
    }

    // Extract EntityMatchType Enum describing match.
    private EntityMatchType extractEntityMatchType(String entity, MatchKeyTuple tuple, List<String> lookupResultList,
                                                   String inputDuns) {
        EntityMatchType entityMatchType;
        if (tuple == null) {
            entityMatchType = EntityMatchType.NO_MATCH;
        } else {
            entityMatchType = EntityMatchType.UNKNOWN;

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
                    if (InterfaceName.CustomerAccountId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Account.name().equals(entity)) {
                        entityMatchType = EntityMatchType.ACCOUNTID;
                        // log.debug("MatchKeyTuple contains CustomerAccountId: " +
                        // systemId.getValue());
                        break;
                    } else if (InterfaceName.CustomerContactId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))
                            && BusinessEntity.Contact.name().equals(entity)) {
                        entityMatchType = EntityMatchType.CONTACTID;
                        // log.debug("MatchKeyTuple contains CustomerContactId: " +
                        // systemId.getValue());
                        break;
                    } else if (InterfaceName.AccountId.name().equals(systemId.getKey())
                            && StringUtils.isNotBlank(systemId.getValue())
                            && BusinessEntity.Contact.name().equals(entity)) {
                        // log.debug("MatchKeyTuple contains AccountId: " + systemId.getValue());
                        hasAccountId = true;
                    } else if (StringUtils.isNotBlank(systemId.getKey()) && StringUtils.isNotBlank(systemId.getValue())
                            && StringUtils.isNotBlank(lookupResultList.get(i))) {
                        entityMatchType = EntityMatchType.SYSTEMID;
                        // log.debug("MatchKeyTuple contains SystemId: " + systemId.getKey() + " with
                        // value: "
                        // + systemId.getValue());
                        break;
                    }
                    i++;
                }
            }

            if (entityMatchType == EntityMatchType.UNKNOWN) {
                if (tuple.hasDomain()) {
                    entityMatchType = EntityMatchType.DOMAIN_COUNTRY;
                } else if (tuple.hasDuns()) {
                    // Set the match type to DUNS if the input has a DUNS value and it equals the DUNS value
                    // the Entity Match system stored in the MatchKeyTuple, which is the final DUNS result.
                    if (StringUtils.isNotBlank(inputDuns) && inputDuns.equals(tuple.getDuns())) {
                        entityMatchType = EntityMatchType.DUNS;
                    } else {
                        entityMatchType = EntityMatchType.LDC_MATCH;
                    }
                } else if (tuple.hasEmail()) {
                    if (hasAccountId) {
                        entityMatchType = EntityMatchType.EMAIL_ACCOUNTID;
                    } else {
                        entityMatchType = EntityMatchType.EMAIL;
                    }
                } else if (tuple.hasName()) {
                    if (BusinessEntity.Account.name().equals(entity)) {
                        entityMatchType = EntityMatchType.NAME_COUNTRY;
                    } else if (tuple.hasPhoneNumber()) {
                        if (hasAccountId) {
                            entityMatchType = EntityMatchType.NAME_PHONE_ACCOUNTID;
                        } else {
                            entityMatchType = EntityMatchType.NAME_PHONE;
                        }
                    }
                }
            }
        }

        // Log the EntityMatchType and matched Entity MatchKeyTuple.
        log.debug("EntityMatchType is: " + entityMatchType);
        log.debug("MatchedEntityMatchKeyTuple: " + tuple);
        if (BusinessEntity.Account.name().equals(entity)) {
            log.debug("    inputDuns: " + inputDuns + "  tupleDuns: " + (tuple == null ? "null" : tuple.getDuns()));
        } else if (EntityMatchType.LDC_MATCH.equals(entityMatchType)) {
            log.error("Found LDC Match type for entity " + entity + " which should not be possible");
            return null;
        }
        return entityMatchType;
    }

    private Pair<LdcMatchType, MatchKeyTuple> extractLdcMatchTypeAndTuple(MatchTraveler traveler) {
        // Get matched DUNS from traveler DUNS map for Account Master entry.
        String matchedDuns = null;
        String latticeAccountId = null;
        if (MapUtils.isNotEmpty(traveler.getDunsOriginMap()) &&
                traveler.getDunsOriginMap().containsKey(DataCloudConstants.ACCOUNT_MASTER)) {
            matchedDuns = traveler.getDunsOriginMap().get(DataCloudConstants.ACCOUNT_MASTER);
        }
        if (traveler.getEntityIds().containsKey(BusinessEntity.LatticeAccount.name())) {
            latticeAccountId = traveler.getEntityIds().get(BusinessEntity.LatticeAccount.name());
        }
        log.debug("    AM Matched DUNS: " + matchedDuns);
        log.debug("    Lattice Account ID: " + latticeAccountId);

        LdcMatchType ldcMatchType = LdcMatchType.NO_MATCH;
        MatchKeyTuple ldcMatchedTuple = null;
        // There must be a Lattice Account ID for LDC Match to have succeeded.
        if (!StringUtils.isBlank(latticeAccountId)) {
            if (CollectionUtils.isEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
                log.error("LDC Match succeeded but entityLdcMatchTypeToTupleList is null or empty");
                return null;
            }

            List<Pair<MatchKeyTuple, List<String>>> ldcMatchLookupResultList = traveler
                    .getEntityMatchLookupResult(BusinessEntity.LatticeAccount.name());
            if (CollectionUtils.isEmpty(ldcMatchLookupResultList)) {
                log.error("LDC Match succeeded but EntityMatchLookupResult for LatticeAccount is null or empty");
                return null;
            }

            if (ldcMatchLookupResultList.size() != traveler.getEntityLdcMatchTypeToTupleList().size()) {
                log.error("EntityMatchLookupResult for {} and EntityLdcMatchTypeToTupleList are not equal length: " +
                                "{} vs {}", BusinessEntity.LatticeAccount.name(), ldcMatchLookupResultList.size(),
                        traveler.getEntityLdcMatchTypeToTupleList().size());
                return null;
            }

            // Iterate through the lists of LDC Match Lookup Results and LDC Match Type /MatchKeyTuple pairs, to find
            // the the first successful result. Then record the corresponding LDC Match Type and MatchKeyTuple of
            // that result.
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
                        ldcMatchType = traveler.getEntityLdcMatchTypeToTupleList().get(i).getLeft();
                        ldcMatchedTuple = traveler.getEntityLdcMatchTypeToTupleList().get(i).getRight();
                        foundResult = true;
                        break;
                    }
                }
            }

            if (!foundResult) {
                log.error("LDC Match succeeded but Lookup Results has no entry with non-null result");
                return null;
            } else if (ldcMatchType == null) {
                log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null LdcMatchType");
                return null;
            } else if (ldcMatchedTuple == null) {
                log.error("EntityLdcMatchTypeToTupleList entry " + i + " has null MatchKeyTuple for type: " + ldcMatchType);
                return null;
            }

            // If the MatchKeyTuple has a DUNS fields and the LdcMatchType is not LDC DUNS or DUNS plus Domain, then the
            // LDC Match has stuck a DUNS value in the matched MatchKeyTuple that wasn't actually part of the input match
            // tuple used for matching. In this case, a copy of the MatchKeyTuple without the DUNS field needs to be
            // created and returned.
            if (ldcMatchedTuple.hasDuns() && !LdcMatchType.DUNS.equals(ldcMatchType)
                    && !LdcMatchType.DUNS_DOMAIN.equals(ldcMatchType)) {
                ldcMatchedTuple = copyLdcTupleNoDuns(ldcMatchedTuple);
            }
        }
        log.debug("LdcMatchType is: " + ldcMatchType);
        log.debug("MatchedLdcMatchKeyTuple: " + ldcMatchedTuple);
        return Pair.of(ldcMatchType, ldcMatchedTuple);
    }

    MatchKeyTuple copyLdcTupleNoDuns(MatchKeyTuple tuple) {
        // Don't set System ID since it is not used in LDC match even if it is set.
        return new MatchKeyTuple.Builder().withDomain(tuple.getDomain()).withName(tuple.getName())
                .withCity(tuple.getCity()).withState(tuple.getState()).withCountry(tuple.getCountry())
                .withCountryCode(tuple.getCountryCode()).withZipcode(tuple.getZipcode())
                .withPhoneNumber(tuple.getPhoneNumber()).withEmail(tuple.getEmail()).build();
    }

    private List<Pair<String, MatchKeyTuple>> extractExistingLookupKeyList(MatchTraveler traveler, String entity) {
        if (MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap())) {
            log.debug("EntityExistingLookupEntryMap is null or empty");
            return null;
        } else if (!traveler.getEntityExistingLookupEntryMap().containsKey(entity)
                || MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap().get(entity))) {
            log.debug("EntityExistingLookupEntryMap for entity " + entity + " is null or empty");
            return new ArrayList<>();
        }
        List<Pair<String, MatchKeyTuple>> existingLookupList = new ArrayList<>();
        for (Map.Entry<EntityMatchType, List<MatchKeyTuple>> typeEntry : traveler.getEntityExistingLookupEntryMap()
                .get(entity).entrySet()) {
            if (CollectionUtils.isEmpty(typeEntry.getValue())) {
                continue;
            }
            for (MatchKeyTuple tuple : typeEntry.getValue()) {
                existingLookupList.add(Pair.of(typeEntry.getKey().name(), tuple));
            }
        }
        return existingLookupList;
    }

    // Assumes traveler.getEntityIds(), traveler.getEntityMatchKeyTuples(), and
    // traveler.getEntityMatchLookupResults() do not return null because they were
    // checked by generateEntityMatchHistory().
    private void generateEntityMatchHistoryDebugLogs(MatchTraveler traveler) {
        log.debug("------------------------ Entity Match History Extra Debug Logs ------------------------");
        log.debug("EntityIds are: ");
        for (Map.Entry<String, String> entry : traveler.getEntityIds().entrySet()) {
            log.debug("   Entity: " + entry.getKey() + "  EntityId: " + entry.getValue());
        }

        if (MapUtils.isEmpty(traveler.getNewEntityIds())) {
            log.debug("Found null or empty NewEntityIds Map in MatchTraveler");
        } else {
            log.debug("NewEntityIds is: ");
            for (Map.Entry<String, String> entry : traveler.getNewEntityIds().entrySet()) {
                log.debug("    Entity: " + entry.getKey() + "  NewEntityId: " + entry.getValue());
            }
        }

        log.debug("EntityMatchKeyTuples is: ");
        for (Map.Entry<String, MatchKeyTuple> entry : traveler.getEntityMatchKeyTuples().entrySet()) {
            log.debug("    Entity: " + entry.getKey());
            log.debug("      MatchKeyTuple: " + entry.getValue().toString());
        }

        log.debug("Iterate through EntityMatchLookupResults:");
        for (Map.Entry<String, List<Pair<MatchKeyTuple, List<String>>>> entry : traveler.getEntityMatchLookupResults()
                .entrySet()) {
            boolean foundResult = false;
            log.debug("    MatchKeyTuple Lookup Results for " + entry.getKey());
            for (Pair<MatchKeyTuple, List<String>> pair : entry.getValue()) {
                if (pair.getKey() == null) {
                    log.debug("        MatchKeyTuple: null");
                } else {
                    log.debug("        MatchKeyTuple: " + pair.getKey().toString());
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
                log.debug("        Results: " + resultList);
            }
        }

        if (CollectionUtils.isNotEmpty(traveler.getEntityLdcMatchTypeToTupleList())) {
            log.debug("Iterate through EntityLdcMatchTypeToTupleList:");
            for (Pair<LdcMatchType, MatchKeyTuple> pair : traveler.getEntityLdcMatchTypeToTupleList()) {
                log.debug("    LdcMatchType: " + pair.getLeft() + "  LdcMatchKeyTuple: " + pair.getRight());
            }
        } else {
            log.debug("EntityLdcMatchTypeToTupleList is empty");
        }

        generateDebugExistingMatchKeyLookups(traveler);
        // log.debug("------------------------ END Entity Match History Extra Debug Logs ------------------------");
    }

    private void generateDebugExistingMatchKeyLookups(MatchTraveler traveler) {
        log.debug("Iterate through existing Entity Match Lookup Keys");
        if (MapUtils.isEmpty(traveler.getEntityExistingLookupEntryMap())) {
            log.debug("   EntityExistingLookupEntryMap is null or empty");
            return;
        }

        for (Map.Entry<String, Map<EntityMatchType, List<MatchKeyTuple>>> entityEntry : traveler
                .getEntityExistingLookupEntryMap().entrySet()) {
            log.debug("  " + entityEntry.getKey() + " - Existing Lookup Entries");
            if (MapUtils.isEmpty(entityEntry.getValue())) {
                log.debug("    <empty>");
                continue;
            }
            for (Map.Entry<EntityMatchType, List<MatchKeyTuple>> typeEntry : entityEntry.getValue().entrySet()) {
                log.debug("    " + typeEntry.getKey() + ":");
                if (CollectionUtils.isEmpty(typeEntry.getValue())) {
                    log.debug("      <empty> (for " + typeEntry.getKey() + ")");
                    continue;
                }
                for (MatchKeyTuple tuple : typeEntry.getValue()) {
                    log.debug("      " + tuple);
                }
            }
        }
    }
}
