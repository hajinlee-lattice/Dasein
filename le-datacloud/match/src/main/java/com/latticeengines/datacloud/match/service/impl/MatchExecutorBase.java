package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.firehose.FirehoseService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.DirectPlusCandidateService;
import com.latticeengines.datacloud.match.service.DisposableEmailService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchCoreErrorConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;
import com.latticeengines.domain.exposed.datacloud.match.VboUsageEvent;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusUsageReportConfig;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

public abstract class MatchExecutorBase implements MatchExecutor {

    private static final Logger log = LoggerFactory.getLogger(MatchExecutorBase.class);

    private static final String FABRIC_MATCH_HISTORY = "FabricMatchHistory";

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DisposableEmailService disposableEmailService;

    @Autowired
    protected MetricService metricService;

    @Inject
    private DirectPlusCandidateService directPlusCandidateService;

    @Value("${datacloud.match.publish.match.history:false}")
    private boolean isMatchHistoryEnabled;

    @Value("${datacloud.match.history.delivery.stream.name}")
    private String deliveryStreamName;

    @Inject
    private FirehoseService firehoseService;

    private ExecutorService publishExecutor;

    @Value("${datacloud.yarn.fetchonly.num.threads}")
    private int publishThreadPool;
    @Value("${datacloud.yarn.fetchonly.queue.size:200}")
    private int queueSize;

    @PostConstruct
    public void init() {
        if (isMatchHistoryEnabled) {
            log.info("MatchHistory is enabled.");
            publishExecutor = ThreadPoolUtils.getBoundedQueueCallerThreadPool(1, publishThreadPool, 1, queueSize);
        }
    }

    @PreDestroy
    public void destroy() {
        if (isMatchHistoryEnabled) {
            log.info("Shutting down match history executors");
            ThreadPoolUtils.shutdownAndAwaitTermination(publishExecutor, 60);
            log.info("Completed shutting match history executors");
        }
    }

    @MatchStep
    protected MatchContext complete(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        matchContext = updateInternalResults(dbHelper, matchContext);
        matchContext = mergeResults(matchContext);
        matchContext.getOutput().setFinishedAt(new Date());
        long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - receiveTime);
        processMatchHistory(matchContext);

        return matchContext;
    }

    @Override
    public MatchContext executeAsync(MatchContext matchContext) {
        return matchContext;
    }

    @Override
    public MatchContext executeMatchResult(MatchContext matchContext) {
        return matchContext;
    }

    @MatchStep
    private MatchContext updateInternalResults(DbHelper dbHelper, MatchContext matchContext) {
        matchContext = dbHelper.updateInternalResults(matchContext);
        return matchContext;
    }

    private void processMatchHistory(MatchContext matchContext) {
        if (!isMatchHistoryEnabled) {
            log.info("MatchHistory not enabled, returning.");
            return;
        }
        List<InternalOutputRecord> records = matchContext.getInternalResults();
        if (CollectionUtils.isEmpty(records)) {
            return;
        }
        List<MatchHistory> matchHistories = new ArrayList<>();
        for (InternalOutputRecord record : records) {
            MatchHistory matchHistory = record.getFabricMatchHistory();
            if (matchHistory == null) {
                continue;
            }
            matchHistory.setRawDomain(record.getOrigDomain()).setRawDUNS(record.getOrigDuns())
                    .setRawEmail(record.getOrigEmail()).setRawSystemIds(record.getOrigSystemIds())
                    .withRawNameLocation(record.getOrigNameLocation());
            matchHistory.setStandardisedDomain(record.getParsedDomain()).setStandardisedDUNS(record.getParsedDuns())
                    .setStandardisedEmail(record.getParsedEmail()).setStandardisedSystemIds(record.getParsedSystemIds())
                    .withStandardisedNameLocation(record.getParsedNameLocation());
            matchHistory.setIsPublicDomain(record.isPublicDomain()).setLatticeAccountId(record.getLatticeAccountId());

            matchHistory.setMatchedDomain(record.getMatchedDomain()).setMatchedEmail(record.getMatchedEmail())
                    .setMatchedDUNS(record.getMatchedDuns()).withMatchedNameLocation(record.getMatchedNameLocation());
            matchHistory.setMatchedEmployeeRange(record.getMatchedEmployeeRange())
                    .setMatchedRevenueRange(record.getMatchedRevenueRange())
                    .setMatchedPrimaryIndustry(record.getMatchedPrimaryIndustry())
                    .setMatchedSecondaryIndustry(record.getMatchedSecondIndustry())
                    .setDomainSource(record.getDomainSource())
                    .setRequestTimestamp(DateTimeUtils.format(record.getRequestTimeStamp()))
                    .setDataCloudVersion(record.getDataCloudVersion());

            // Add EntityMatchHistory to MatchHistory.
            matchHistory.setEntityMatchHistory(record.getEntityMatchHistory());

            MatchInput matchInput = matchContext.getInput();
            if (matchInput != null) {
                if (matchInput.getTenant() != null) {
                    matchHistory.setTenantId(CustomerSpace.shortenCustomerSpace(matchInput.getTenant().getId()))
                            .setRootOperationUid(matchInput.getRootOperationUid())
                            .setApplicationId(matchInput.getApplicationId());
                }
                if (matchInput.getRequestSource() != null)
                    matchHistory.setRequestSource(matchInput.getRequestSource().toString());
                if (matchInput.isFetchOnly() && //
                        (record.getLatticeAccount() != null || (record.getFetchResult() != null && !record.getFetchResult().getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE)))) {
                    matchHistory.setMatched(true);
                    matchHistory.setLdcMatched(true);
                }
            }
            String matchStatus = getMatchStatus(record, matchHistory);
            matchHistory.setMatchStatus(matchStatus);

            matchHistories.add(matchHistory);
        }

        publishMatchHistory(matchHistories);
    }

    private String getMatchStatus(InternalOutputRecord record, MatchHistory matchHistory) {
        if (record.isFailed()) {
            return "U";
        } else {
            return Boolean.TRUE.equals(matchHistory.getMatched()) ? "T" : "F";
        }
    }

    private void publishMatchHistory(List<MatchHistory> matchHistories) {
        if (CollectionUtils.isEmpty(matchHistories)) {
            return;
        }
        Runnable runnable = () -> {
            try {
                for (MatchHistory matchHistory : matchHistories) {
                    GenericRecordRequest recordRequest = new GenericRecordRequest();
                    recordRequest.setId(UUID.randomUUID().toString());
                    matchHistory.setId(recordRequest.getId());
                    recordRequest.setStores(Collections.singletonList(FabricStoreEnum.S3))
                            .setRepositories(Collections.singletonList(FABRIC_MATCH_HISTORY))
                            .setBatchId(FABRIC_MATCH_HISTORY);
                }
                List<String> histories = new ArrayList<>();
                matchHistories.forEach(e -> histories.add(JsonUtils.serialize(e)));
                firehoseService.sendBatch(deliveryStreamName, histories);
            } catch (Exception ex) {
                log.warn("Failed to publish match history! error=" + ex.getMessage());
            }
        };
        try {
            publishExecutor.execute(runnable);
        } catch (Throwable t) {
            log.warn("Failed to publish match history!", t);
        }

    }

    @VisibleForTesting
    @MatchStep
    MatchContext mergeResults(MatchContext matchContext) {
        List<InternalOutputRecord> records = matchContext.getInternalResults();
        List<Column> columns = matchContext.getColumnSelection().getColumns();
        List<String> inputFields = matchContext.getInput().getFields();
        boolean returnUnmatched = matchContext.isReturnUnmatched();
        boolean excludeUnmatchedPublicDomain = Boolean.TRUE.equals(matchContext.getInput().getExcludePublicDomain());
        boolean isPrimeMatch = BusinessEntity.PrimeAccount.name().equals(matchContext.getInput().getTargetEntity());
        DplusUsageReportConfig usageReportConfig = matchContext.getInput().getDplusUsageReportConfig();
        boolean shouldReportUsage = usageReportConfig != null && usageReportConfig.isEnabled();

        List<OutputRecord> outputRecords = new ArrayList<>();
        Integer[] columnMatchCount = new Integer[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnMatchCount[i] = 0;
        }

        int totalMatched = 0;
        long orphanedNoMatchCount = 0L;
        long orphanedUnmatchedAccountIdCount = 0L;
        long matchedByMatchKeyCount = 0L;
        long matchedByAccountIdCount = 0L;
        Map<String, Long> nullEntityIdCount = new HashMap<>();
        Map<String, Long> newEntityCnt = new HashMap<>();

        boolean isEntityMatch = OperationalMode.isEntityMatch(matchContext.getInput().getOperationalMode());
        if (isEntityMatch) {
            matchContext.setDomains(new HashSet<>());
            matchContext.setNameLocations(new HashSet<>());
        }

        log.info("No. columns = {}, no. records = {}", columns.size(), records.size());

        // Convert InternalOutputRecords to OutputRecords
        for (InternalOutputRecord internalRecord : records) {
            if (internalRecord.isFailed()) {
                OutputRecord outputRecord = new OutputRecord();
                outputRecord.setInput(internalRecord.getInput());
                outputRecord.setMatched(false);
                outputRecord.setRowNumber(internalRecord.getRowNumber());
                outputRecord.setMatchLogs(internalRecord.getMatchLogs());
                outputRecord.setErrorMessages(internalRecord.getErrorMessages());
                outputRecords.add(outputRecord);
                continue;
            }


            List<Map<String, Object>> queryResults = new LinkedList<>();
            List<List<Object>> outputList = new LinkedList<>();
            if (isMultiResultMatch(matchContext)) {
                queryResults = internalRecord.getMultiQueryResult();
                if (queryResults.size() > 1000) {
                    log.info("Multi query result size = {}", queryResults.size());
                }
                // clear these internal field, otherwise deep copy might take a long time
                internalRecord.setMultiQueryResult(null);
                internalRecord.setMultiFetchResults(null);
            } else {
                queryResults.add(internalRecord.getQueryResult());
            }

            boolean firstRead = true;
            for (Map<String, Object> results: queryResults) {
                 InternalOutputRecord copy;
                 if (firstRead) {
                     copy = internalRecord;
                     firstRead = false;
                 } else {
                     copy = internalRecord.deepCopy();
                 }

                List<Object> output = populateOutputData(copy, results, //
                        matchContext, columnMatchCount, nullEntityIdCount);
                outputList.add(output);
                internalRecord.setOutput(output);

                if (CollectionUtils.isNotEmpty(internalRecord.getFieldsToClear())) {
                    // clear out specific fields in input (copy new row for now to avoid affecting
                    // input object)
                    List<Object> clearedInput = new ArrayList<>();
                    for (int i = 0; i < inputFields.size(); i++) {
                        if (internalRecord.getFieldsToClear().contains(inputFields.get(i))) {
                            clearedInput.add(null);
                        } else {
                            clearedInput.add(internalRecord.getInput().get(i));
                        }
                    }
                    internalRecord.setInput(clearedInput);
                }
                // count newly allocated entities
                if (MapUtils.isNotEmpty(copy.getNewEntityIds())) {
                    for (Map.Entry<String, String> entry : copy.getNewEntityIds().entrySet()) {
                        String entity = entry.getKey();
                        String entityId = entry.getValue();
                        if (StringUtils.isNotBlank(entityId)) {
                            newEntityCnt.put(entity, newEntityCnt.getOrDefault(entity, 0L) + 1);
                        }
                    }
                }

                // For LDC and Entity match, IsMatched flag is marked in
                // FuzzyMatchServiceImpl.fetchIdResult
                if (copy.isMatched()) {
                    totalMatched++;

                }
                copy.setResultsInPartition(null);
            }
            OutputRecord outputRecord = new OutputRecord();
            if (returnUnmatched || internalRecord.isMatched()) {
                if (excludeUnmatchedPublicDomain && internalRecord.isPublicDomain()) {
                    log.warn("Excluding the record, because it is using the public domain: "
                            + internalRecord.getParsedDomain());
                } else if (isMultiResultMatch(matchContext)) {
                    outputRecord.setCandidateOutput(outputList);
                } else {
                    outputRecord.setOutput(internalRecord.getOutput());
                }
            }

            outputRecord.setInput(internalRecord.getInput());
            outputRecord.setMatched(internalRecord.isMatched());

            outputRecord.setPreMatchDomain(internalRecord.getParsedDomain());
            outputRecord.setPreMatchNameLocation(internalRecord.getParsedNameLocation());
            outputRecord.setPreMatchDuns(internalRecord.getParsedDuns());
            outputRecord.setPreMatchEmail(internalRecord.getParsedEmail());

            outputRecord.setMatchedDomain(internalRecord.getMatchedDomain());
            outputRecord.setMatchedNameLocation(internalRecord.getMatchedNameLocation());
            outputRecord.setMatchedDuns(internalRecord.getMatchedDuns());
            outputRecord.setMatchedDduns(internalRecord.getMatchedDduns());
            outputRecord.setDnbCacheIds(internalRecord.getDnbCacheIds());
            outputRecord.setNewEntityIds(internalRecord.getNewEntityIds());
            outputRecord.setMatchedEmail(internalRecord.getMatchedEmail());
            outputRecord.setMatchedLatticeAccountId(
                    StringStandardizationUtils.getStandardizedOutputLatticeID(internalRecord.getLatticeAccountId()));

            if (isPrimeMatch) {
                populateCandidateData(outputRecord, internalRecord);
            }

            if (shouldReportUsage) {
                String poaeIdField = usageReportConfig.getPoaeIdField();
                if (StringUtils.isNotBlank(poaeIdField)) {
                    int poaeIdIdx = inputFields.indexOf(poaeIdField);
                    if (poaeIdIdx >= 0) {
                        Object poaeIdVal = internalRecord.getInput().get(poaeIdIdx);
                        String poaeIdStr = null;
                        try {
                            if (poaeIdVal != null) {
                                poaeIdStr = String.valueOf(poaeIdVal);
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse POAEID from object " + poaeIdVal);
                        }
                        if (poaeIdVal == null) {
                            poaeIdStr = UUID.randomUUID().toString().toLowerCase();
                            log.warn("Not able to parse POAEID from input, use random uuid instead: " + poaeIdStr);
                        }
                        populateUsageEvents(outputRecord, internalRecord, poaeIdStr);
                    } else {
                        throw new IllegalArgumentException("Cannot find POAEID field " + poaeIdField //
                                + " from input fields.");
                    }
                }
            }

            // don't copy errors if null or was successful on a later retry
            if (CollectionUtils.isNotEmpty(internalRecord.getErrorMessages()) && internalRecord.getRawError() != null) {
                outputRecord.addErrorCode(MatchCoreErrorConstants.ErrorType.MATCH_ERROR, internalRecord.getRawError());
            }
            if (internalRecord.getFetchResult() != null && internalRecord.getFetchResult().getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE)) {
                outputRecord.addErrorCode(MatchCoreErrorConstants.ErrorType.APPEND_ERROR, (String) internalRecord.getFetchResult().getResult().get(PrimeAccount.ENRICH_ERROR_CODE));
            }

            outputRecord.setRowNumber(internalRecord.getRowNumber());
            outputRecord.setErrorMessages(internalRecord.getErrorMessages());
            outputRecord.setMatchLogs(internalRecord.getMatchLogs());
            outputRecord.setDebugValues(internalRecord.getDebugValues());
            outputRecord.setNumFeatureValue(internalRecord.getNumFeatureValue());

            outputRecords.add(outputRecord);
        }

        if (isPrimeMatch) {
            matchContext.getOutput().setCandidateOutputFields(candidateOutputFields());
        }
        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        log.debug("TotalMatched: " + totalMatched);
        if (isEntityMatch) {
            matchContext.getOutput().getStatistics().setNewEntityCount(newEntityCnt);
            matchContext.getOutput().getStatistics().setOrphanedNoMatchCount(orphanedNoMatchCount);
            matchContext.getOutput().getStatistics()
                    .setOrphanedUnmatchedAccountIdCount(orphanedUnmatchedAccountIdCount);
            matchContext.getOutput().getStatistics().setMatchedByMatchKeyCount(matchedByMatchKeyCount);
            matchContext.getOutput().getStatistics().setMatchedByAccountIdCount(matchedByAccountIdCount);
            if (MapUtils.isNotEmpty(matchContext.getOutput().getStatistics().getNullEntityIdCount())) {
                log.warn("Found null entity id count map in existing context {}, merging with current map {}",
                        matchContext.getOutput().getStatistics().getNullEntityIdCount(), nullEntityIdCount);
                MatchUtils.mergeEntityCnt(nullEntityIdCount,
                        matchContext.getOutput().getStatistics().getNullEntityIdCount());
            }

            matchContext.getOutput().getStatistics().setNullEntityIdCount(nullEntityIdCount);

            log.debug("NewEntityCnt: {}", newEntityCnt);
            log.debug("OrphanedNoMatchCount: " + orphanedNoMatchCount);
            log.debug("OrphanedUnmatchedAccountIdCount: " + orphanedUnmatchedAccountIdCount);
            log.debug("MatchedByMatchKeyCount: " + matchedByMatchKeyCount);
            log.debug("MatchedByAccountIdCount: " + matchedByAccountIdCount);
            log.debug("NullEntityIdCount: {}", nullEntityIdCount);
        }

        if (columnMatchCount.length <= 10000) {
            matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));
        }

        return matchContext;
    }

    private List<Object> populateOutputData(InternalOutputRecord internalRecord, Map<String, Object> results,
                                    MatchContext matchContext,
                                    Integer[] columnMatchCount,
                                    Map<String, Long> nullEntityIdCount) {
        List<String> columnNames = matchContext.getColumnSelection().getColumnIds();
        List<Column> columns = matchContext.getColumnSelection().getColumns();
        List<String> inputFields = matchContext.getInput().getFields();
        String targetEntity = matchContext.getInput().getTargetEntity();

        int templateFieldIdx = inputFields.indexOf(MatchConstants.ENTITY_TEMPLATE_FIELD);
        boolean isAllocateMode = matchContext.getInput().isAllocateId();
        boolean isEntityMatch = OperationalMode.isEntityMatch(matchContext.getInput().getOperationalMode());

        if (isAllocateMode) {
            boolean hasNullId = checkNullEntityIds(internalRecord, targetEntity,
                    nullEntityIdCount);
            if (hasNullId) {
                log.error("Got null entity IDs ({}) when matching record {}", internalRecord.getEntityIds(),
                        internalRecord.getInput());
            }
        }
        internalRecord.setColumnMatched(new ArrayList<>());
        List<Object> output = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String field = column.getExternalColumnId() == null ? column.getColumnName()
                    : column.getExternalColumnId();

            Object value = null;

            if (InterfaceName.EntityId.name().equalsIgnoreCase(field)) {
                // retrieve entity ID (for entity match)
                value = internalRecord.getEntityId();
            } else if (MatchConstants.LID_FIELD.equalsIgnoreCase(field)) {
                if (StringUtils.isNotEmpty(internalRecord.getLatticeAccountId())) {
                    value = StringStandardizationUtils
                            .getStandardizedOutputLatticeID(internalRecord.getLatticeAccountId());
                }
                if (value == null && isEntityMatch) {
                    // try to retrieve lattice account ID from entityId map next
                    value = StringStandardizationUtils.getStandardizedOutputLatticeID(
                            getEntityId(internalRecord, BusinessEntity.LatticeAccount.name()));
                }
            } else if (MatchConstants.IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)
                    && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                    && publicDomainService.isPublicDomain(internalRecord.getParsedDomain())) {
                value = true;
            } else if (MatchConstants.DISPOSABLE_EMAIL.equalsIgnoreCase(field)
                    && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                    && disposableEmailService.isDisposableEmailDomain(internalRecord.getParsedDomain())) {
                value = true;
            } else if (InterfaceName.IsMatched.name().equalsIgnoreCase(field)) {
                value = internalRecord.isMatched();
            } else if (ColumnSelection.Predefined.LeadToAcct
                    .equals(matchContext.getInput().getPredefinedSelection())
                    && InterfaceName.AccountId.name().equalsIgnoreCase(field)) {
                throw new UnsupportedOperationException("Should not be using LeadToAcct match any more.");
            }else if (InterfaceName.AccountId.name().equalsIgnoreCase(field)) {
                // retrieve Account EntityId (for entity match)
                value = getEntityId(internalRecord, Account.name());
            } else if (InterfaceName.ContactId.name().equalsIgnoreCase(field)) {
                // retrieve Contact EntityId (for entity match)
                value = getEntityId(internalRecord, Contact.name());
            } else if (InterfaceName.CDLCreatedTemplate.name().equalsIgnoreCase(field)) {
                String newEntityId = MapUtils.emptyIfNull(internalRecord.getNewEntityIds()).get(targetEntity);
                boolean isNewEntity = StringUtils.isNotBlank(newEntityId);
                if (templateFieldIdx >= 0 && isNewEntity) {
                    // only set created template field for new entity
                    value = internalRecord.getInput().get(templateFieldIdx);
                }
            } else if (results.containsKey(field)) {
                Object objInResult = results.get(field);
                value = (objInResult == null ? value : objInResult);
            }

            try {
                value = MatchOutputStandardizer.cleanNewlineCharacters(value);
            } catch (Exception exc) {
                log.error("Failed to clean up new line characters. Exception: " + exc);
            }

            output.add(value);
            columnMatchCount[i] += (value == null ? 0 : 1);
            internalRecord.getColumnMatched().add(value != null);
        }

        // make *_IsMatched columns not null
        for (int i = 0; i < columnNames.size(); i++) {
            String field = columnNames.get(i);
            Object value = output.get(i);
            if (field.toLowerCase().contains("ismatched") && value == null) {
                output.set(i, false);
            }
            if (MatchConstants.IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)) {
                output.set(i, publicDomainService.isPublicDomain(internalRecord.getParsedDomain()));
            }
            if (MatchConstants.DISPOSABLE_EMAIL.equalsIgnoreCase(field)) {
                output.set(i, disposableEmailService.isDisposableEmailDomain(internalRecord.getParsedDomain()));
            }
            if (MatchConstants.PREMATCH_DOMAIN.equalsIgnoreCase(field)) {
                output.set(i, internalRecord.getParsedDomain());
            }
        }
        return output;
    }

    private boolean isMultiResultMatch(MatchContext matchContext) {
        return OperationalMode.CONTACT_MATCH.equals(matchContext.getInput().getOperationalMode());
    }

    // return whether there are null entity ids in this record
    private boolean checkNullEntityIds(@NotNull InternalOutputRecord record, @NotNull String entity,
            @NotNull Map<String, Long> nullEntityIdCount) {
        if (Contact.name().equals(entity)) {
            String accId = getEntityId(record, Account.name());
            String ctkId = getEntityId(record, Contact.name());
            if (accId == null) {
                nullEntityIdCount.put(Account.name(), nullEntityIdCount.getOrDefault(Account.name(), 0L) + 1);
            }
            if (ctkId == null) {
                nullEntityIdCount.put(Contact.name(), nullEntityIdCount.getOrDefault(Contact.name(), 0L) + 1);
            }
            return accId == null || ctkId == null;
        } else if (Account.name().equals(entity)) {
            String accId = getEntityId(record, Account.name());
            if (accId == null) {
                nullEntityIdCount.put(Account.name(), nullEntityIdCount.getOrDefault(Account.name(), 0L) + 1);
                return true;
            }
        }
        return false;
    }

    private void populateCandidateData(OutputRecord outputRecord, InternalOutputRecord internalRecord) {
        if (CollectionUtils.isEmpty(internalRecord.getCandidates())) {
            return;
        }
        List<DnBMatchCandidate> candidates = internalRecord.getCandidates();
        List<List<Object>> candidateData = new ArrayList<>();
        for (DnBMatchCandidate candidate: candidates) {
            List<Object> data = directPlusCandidateService.parseCandidate(candidate);
            candidateData.add(data);
        }
        outputRecord.setCandidateOutput(candidateData);
    }

    private void populateUsageEvents(OutputRecord outputRecord, InternalOutputRecord internalRecord, String poaeIdStr) {
        if (CollectionUtils.isNotEmpty(internalRecord.getUsageEvents())) {
            List<VboUsageEvent> usageEvents = new ArrayList<>(internalRecord.getUsageEvents());
            for (VboUsageEvent event: usageEvents) {
                event.setPoaeId(poaeIdStr);
            }
            outputRecord.setUsageEvents(usageEvents);
        }
    }

    private List<String> candidateOutputFields() {
        return directPlusCandidateService.candidateOutputFields();
    }

    private String getEntityId(InternalOutputRecord record, String entity) {
        if (record == null || MapUtils.isEmpty(record.getEntityIds()) || StringUtils.isBlank(entity)) {
            return null;
        }

        return record.getEntityIds().get(entity);
    }
}
