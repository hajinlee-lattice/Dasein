package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.util.EntityMatchUtils.restoreInvalidMatchFieldCharacters;
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
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.datacloud.match.util.MatchHistoryUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    @Lazy
    @Inject
    private MatchMetricService matchMetricService;

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
                boolean ldcMatched = false;
                String result = (String) traveler.getResult();
                if (OperationalMode.isEntityMatch(traveler.getMatchInput().getOperationalMode())) {
                    populateEntityMatchRecordWithTraveler(traveler, result, matchRecord);
                } else if (OperationalMode.PRIME_MATCH.equals(traveler.getMatchInput().getOperationalMode())) {
                    populatePrimeMatchResult(traveler, result, matchRecord);
                } else {
                    matchRecord.setLatticeAccountId(result);
                    if (StringUtils.isNotEmpty(result)) {
                        matchRecord.setMatched(true);
                        ldcMatched = true;
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
                    matchRecord.setFabricMatchHistory(getDnbMatchHistory(matchRecord, traveler, ldcMatched));
                }
                traveler.finish();
                dumpTravelStory(matchRecord, traveler, logLevel);
                dumpEntityMatchErrors(matchRecord, traveler);
            }
        }

        writeFuzzyMatchHistory(fuzzyMatchHistories);
    }

    private void populatePrimeMatchResult(MatchTraveler traveler, String result,
                                          InternalOutputRecord matchRecord) {
        matchRecord.setLatticeAccountId(result);
        if (StringUtils.isNotEmpty(result)) {
            matchRecord.setMatched(true);
        } else {
            matchRecord.addErrorMessages("Cannot find a match in data cloud for the input.");
        }
        matchRecord.setCandidates(traveler.getCandidates());
    }

    private void populateEntityMatchRecordWithTraveler(MatchTraveler traveler, String result,
                                                       InternalOutputRecord matchRecord) {
        // restore if result/entityIds has invalid chars and transformed into
        // placeholder tokens
        matchRecord.setEntityId(EntityMatchUtils.restoreInvalidMatchFieldCharacters(result));
        if (MapUtils.isNotEmpty(traveler.getNewEntityIds())) {
            // copy new entity IDs map
            matchRecord.setNewEntityIds(restoreInvalidMatchFieldCharacters(traveler.getNewEntityIds()));
        }
        matchRecord.setEntityIds(restoreInvalidMatchFieldCharacters(traveler.getEntityIds()));
        if (MapUtils.isNotEmpty(traveler.getEntityIds()) && matchRecord.getLatticeAccountId() == null) {
            String latticeAccountId = traveler.getEntityIds().get(BusinessEntity.LatticeAccount.name());
            matchRecord.setLatticeAccountId(latticeAccountId);
        }
        matchRecord.setFieldsToClear(traveler.getFieldsToClear());
        // Copy data from EntityMatchKeyRecord in MatchTraveler that was set by
        // MatchPlannerMicroEngineActor into the InternalOutputRecord.
        copyFromEntityToInternalOutputRecord(traveler.getEntityMatchKeyRecord(), matchRecord);

        EntityMatchHistory entityMatchHistory = generateEntityMatchHistory(traveler);
        if (entityMatchHistory != null) {
            MatchHistoryUtils.processPreferredIds(matchRecord, traveler, entityMatchHistory);
            // Need to copy information from MatchTraveler to a place where we can add it to MatchHistory.
            matchRecord.setEntityMatchHistory(entityMatchHistory);
        }
        if (StringUtils.isNotEmpty(result) && !DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(result)) {
            matchRecord.setMatched(true);
        }
    }

    private MatchHistory getDnbMatchHistory(InternalOutputRecord matchRecord, MatchTraveler traveler,
            boolean ldcMatched) {
        MatchHistory matchHistory = matchRecord.getFabricMatchHistory();
        matchHistory.setMatched(traveler.isMatched()).setLdcMatched(ldcMatched).setMatchMode(traveler.getMode());
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
                    debugValues.add(matchContext.isACPassedString());
                    debugValues.add(matchContext.isDunsInAMString());
                    debugValues.add(matchContext.isOutOfBusinessString());
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
                MatchTraveler matchTraveler;
                if (OperationalMode.isEntityMatch(matchInput.getOperationalMode())) {
                    matchTraveler = new MatchTraveler(matchInput.getRootOperationUid(), null);
                    matchTraveler.setInputDataRecord(matchRecord.getInput());
                    matchTraveler.setEntityKeyPositionMaps(matchRecord.getEntityKeyPositionMaps());
                    EntityMatchKeyRecord entityMatchKeyRecord = new EntityMatchKeyRecord();
                    entityMatchKeyRecord.setOrigTenant(matchRecord.getOrigTenant());
                    entityMatchKeyRecord.setParsedTenant(matchRecord.getParsedTenant());
                    matchTraveler.setEntityMatchKeyRecord(entityMatchKeyRecord);
                } else {
                    MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
                    matchTraveler = new MatchTraveler(matchInput.getRootOperationUid(), matchKeyTuple);
                    String domain = matchKeyTuple.getDomain();
                    if (StringUtils.isNotBlank(domain)) {
                        domainCollectService.enqueue(domain);
                    }
                }
                if (StringUtils.isNotBlank(matchInput.getTargetEntity())) {
                    // for entity match, 1st decision graph's entity is just final target entity
                    matchTraveler.setEntity(matchInput.getTargetEntity());
                } else {
                    matchTraveler.setEntity(BusinessEntity.LatticeAccount.name());
                }
                matchTraveler.setOperationalMode(matchInput.getOperationalMode());
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
            if (CollectionUtils.isNotEmpty(metrics)) {
                metrics.forEach(matchMetricService::recordFuzzyMatchRecord);
            }
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
    private void copyFromEntityToInternalOutputRecord(EntityMatchKeyRecord entityRecord, InternalOutputRecord internalRecord) {
        internalRecord.setParsedDomain(entityRecord.getParsedDomain());
        internalRecord.setPublicDomain(entityRecord.isPublicDomain());
        internalRecord.setMatchEvenIsPublicDomain(entityRecord.isMatchEvenIsPublicDomain());
        internalRecord.setParsedDuns(entityRecord.getParsedDuns());
        internalRecord.setParsedNameLocation(entityRecord.getParsedNameLocation());
        internalRecord.setParsedEmail(entityRecord.getParsedEmail());
        internalRecord.setParsedTenant(entityRecord.getParsedTenant());
        internalRecord.setParsedPreferredEntityIds(entityRecord.getParsedPreferredEntityIds());
        internalRecord.setParsedSystemIds(entityRecord.getParsedSystemIds());
        internalRecord.setOrigDomain(entityRecord.getOrigDomain());
        internalRecord.setOrigNameLocation(entityRecord.getOrigNameLocation());
        internalRecord.setOrigDuns(entityRecord.getOrigDuns());
        internalRecord.setOrigEmail(entityRecord.getOrigEmail());
        internalRecord.setOrigTenant(entityRecord.getOrigTenant());
        internalRecord.setFailed(entityRecord.isFailed());
        internalRecord.setErrorMessages(entityRecord.getErrorMessages());
        internalRecord.setOrigSystemIds(entityRecord.getOrigSystemIds());
        internalRecord.setOrigPreferredEntityIds(entityRecord.getOrigPreferredEntityIds());
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

    // TODO(jwinter): Handle error conditions more gracefully.  Try to build as much of the EntityMatchHistory as
    //     possible despite the error, rather than just returning null.  Also, provide log errors when returning
    //     null explaining the issue, along with better debugging logs (print out data structures).
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

        history.setAllocateIdMode(traveler.getMatchInput().isAllocateId());
        log.debug("Allocate Id Mode: " + history.isAllocateIdMode());

        // Check if DUNS was a provided MatchKey and extract Input MatchKeys
        String inputDuns = getInputDunsAndPrintInputMatchKeys(traveler, history.getBusinessEntity());

        // Extract the matched Entity ID and whether there was a match.
        history.setEntityId(extractEntityId(traveler, history.getBusinessEntity()));
        history.setEntityMatched(extractMatchedState(traveler, history.getBusinessEntity(), history.getEntityId()));

        // Get Full MatchKeyTuple for Business Entity.
        history.setFullMatchKeyTuple(extractFullMatchKeyTuple(traveler, history.getBusinessEntity()));
        if (history.getFullMatchKeyTuple() == null) {
            log.error("Traveler missing FullMatchKeyTuple for entity " + history.getBusinessEntity());
            return null;
        }

        // Get Customer Entity Id, if provided.
        history.setCustomerEntityId(
                extractCustomerEntityId(history.getFullMatchKeyTuple(), history.getBusinessEntity()));

        // Get MatchKeyTuple that found Entity ID, if a match was found.
        if (!checkEntityMatchLookupResults(traveler, history.getBusinessEntity())) {
            log.error("Traveler EntityMatchLookupResults is malformed.");
            return null;
        }
        List<String> lookupResultList = new ArrayList<>();
        history.setMatchedEntityMatchKeyTuple(
                extractMatchedMatchKeyTuple(traveler, history.getBusinessEntity(), lookupResultList));
        // Generate EntityMatchType Enum describing the match.
        history.setEntityMatchType(MatchHistoryUtils.extractEntityMatchType(history.getBusinessEntity(),
                history.getMatchedEntityMatchKeyTuple(), lookupResultList, inputDuns));
        if (history.getEntityMatchType() == null) {
            log.error("Could not determine EntityMatchType");
            return null;
        }

        if (BusinessEntity.Account.name().equals(history.getBusinessEntity())) {
            // Now set the LdcMatchType and the MatchedLdcMatchKeyTuple if LDC Match succeeded.
            Pair<LdcMatchType, MatchKeyTuple> typeTuplePair = MatchHistoryUtils.extractLdcMatchTypeAndTuple(traveler);
            if (typeTuplePair == null) {
                log.error("Could not extract LDC Match Type and Tuple");
                return null;
            }
            history.setLdcMatchType(typeTuplePair.getLeft());
            history.setMatchedLdcMatchKeyTuple(typeTuplePair.getRight());
        }

        // Generate list of all Existing Entity Lookup Keys.
        history.setExistingLookupKeyList(MatchHistoryUtils.extractExistingLookupKeyList(
                traveler, history.getBusinessEntity()));

        // Add LeadToAccount Matching Results for Contacts.
        if (BusinessEntity.Contact.name().equals(history.getBusinessEntity())) {
            log.debug("------------------------ BEGIN L2A Match History Debug Logs ------------------------");
            if (!generateL2aMatchHistory(traveler, history)) {
                log.error("Failed to generate Lead-to-Account Entity Match History");
                return null;
            }
        }

        // Log extra debug information about the match.
        MatchHistoryUtils.generateEntityMatchHistoryDebugLogs(traveler);

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
            log.debug("Traveler missing Lead-to-Account FullMatchKeyTuple");
            return false;
        }

        // Get Customer Entity Id, if provided.
        history.setL2aCustomerEntityId(extractCustomerEntityId(history.getL2aFullMatchKeyTuple(), accountEntity));

        // Get MatchKeyTuple that found Entity ID, if a match was found.
        if (!checkEntityMatchLookupResults(traveler, accountEntity)) {
            log.debug("Traveler Lead-to-Account EntityMatchLookupResults is malformed.");
            return false;
        }
        List<String> lookupResultList = new ArrayList<>();
        history.setL2aMatchedEntityMatchKeyTuple(extractMatchedMatchKeyTuple(traveler, accountEntity, lookupResultList));
        // Generate L2A EntityMatchType Enum describing the match.
        history.setL2aEntityMatchType( MatchHistoryUtils.extractEntityMatchType(accountEntity,
                history.getL2aMatchedEntityMatchKeyTuple(), lookupResultList, inputDuns));
        if (history.getL2aEntityMatchType() == null) {
            log.error("Could not determine Lead-to-Account EntityMatchType");
            return false;
        }

        // Now set the L2aLdcMatchType and the L2aMatchedLdcMatchKeyTuple if LDC Match succeeded.
        Pair<LdcMatchType, MatchKeyTuple> typeTuplePair = MatchHistoryUtils.extractLdcMatchTypeAndTuple(traveler);
        if (typeTuplePair == null) {
            log.debug("Could not extract Lead-to-Account LDC Match Type and Tuple");
            return false;
        }
        history.setL2aLdcMatchType(typeTuplePair.getLeft());
        history.setL2aMatchedLdcMatchKeyTuple(typeTuplePair.getRight());

        // Generate list of all Existing Entity Lookup Keys.
        history.setL2aExistingLookupKeyList(MatchHistoryUtils.extractExistingLookupKeyList(traveler, accountEntity));
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

}
