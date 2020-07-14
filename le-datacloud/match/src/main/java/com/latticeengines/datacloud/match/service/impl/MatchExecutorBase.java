package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
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
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.DirectPlusCandidateService;
import com.latticeengines.datacloud.match.service.DisposableEmailService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
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

    @PostConstruct
    public void init() {
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
                        (record.getLatticeAccount() != null || record.getPrimeAccount() != null)) {
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
        if (!isMatchHistoryEnabled) {
            log.info("MatchHistory not enabled, returning.");
            return;
        }
        if (CollectionUtils.isEmpty(matchHistories)) {
            return;
        }
        for (MatchHistory matchHistory : matchHistories) {
            GenericRecordRequest recordRequest = new GenericRecordRequest();
            recordRequest.setId(UUID.randomUUID().toString());
            matchHistory.setId(recordRequest.getId());
            recordRequest.setStores(Collections.singletonList(FabricStoreEnum.S3))
                    .setRepositories(Collections.singletonList(FABRIC_MATCH_HISTORY)).setBatchId(FABRIC_MATCH_HISTORY);
        }
        List<String> histories = new ArrayList<>();
        matchHistories.forEach(e -> histories.add(JsonUtils.serialize(e)));
        log.debug("Firehose delivery stream " + deliveryStreamName + " publishing MatchHistory");
        firehoseService.sendBatch(deliveryStreamName, histories);
    }

    @VisibleForTesting
    @MatchStep
    MatchContext mergeResults(MatchContext matchContext) {
        List<InternalOutputRecord> records = matchContext.getInternalResults();
        List<String> columnNames = matchContext.getColumnSelection().getColumnIds();
        List<Column> columns = matchContext.getColumnSelection().getColumns();
        List<String> inputFields = matchContext.getInput().getFields();
        int templateFieldIdx = inputFields.indexOf(MatchConstants.ENTITY_TEMPLATE_FIELD);
        boolean returnUnmatched = matchContext.isReturnUnmatched();
        boolean excludeUnmatchedPublicDomain = Boolean.TRUE.equals(matchContext.getInput().getExcludePublicDomain());
        boolean isAllocateMode = matchContext.getInput().isAllocateId();
        boolean isPrimeMatch = BusinessEntity.PrimeAccount.name().equals(matchContext.getInput().getTargetEntity());

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

            internalRecord.setColumnMatched(new ArrayList<>());
            List<Object> output = new ArrayList<>();

            Map<String, Object> results = internalRecord.getQueryResult();
            if (isAllocateMode) {
                boolean hasNullId = checkNullEntityIds(internalRecord, matchContext.getInput().getTargetEntity(),
                        nullEntityIdCount);
                if (hasNullId) {
                    log.error("Got null entity IDs ({}) when matching record {}", internalRecord.getEntityIds(),
                            internalRecord.getInput());
                }
            }

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
                    // For Lead-to-Account match, if cannot find matched AccountId or customer's
                    // AccountId doesn't match with AccountId from matcher, return anonymous
                    // AccountId to help ProfileContact step which requires existence of AccountId.
                    // Anonymous AccountId is some predefined string which should have very low
                    // chance to be conflict with real AccountId. And these contacts become orphan.
                    value = results.get(field);
                    String customerAccountId = internalRecord.getParsedSystemIds() == null ? null
                            : internalRecord.getParsedSystemIds().get(InterfaceName.AccountId.name());
                    // Record match result in enumeration for aggregation into match report.
                    if (value == null) {
                        orphanedNoMatchCount++;
                    } else if (customerAccountId == null) {
                        matchedByMatchKeyCount++;
                    } else if (value.equals(customerAccountId)) {
                        matchedByAccountIdCount++;
                    } else {
                        orphanedUnmatchedAccountIdCount++;
                    }
                    if (value == null || (customerAccountId != null && !value.equals(customerAccountId))) {
                        value = DataCloudConstants.ENTITY_ANONYMOUS_AID;
                    }
                } else if (InterfaceName.AccountId.name().equalsIgnoreCase(field)) {
                    // retrieve Account EntityId (for entity match)
                    value = getEntityId(internalRecord, Account.name());
                } else if (InterfaceName.ContactId.name().equalsIgnoreCase(field)) {
                    // retrieve Contact EntityId (for entity match)
                    value = getEntityId(internalRecord, Contact.name());
                } else if (InterfaceName.CDLCreatedTemplate.name().equalsIgnoreCase(field)) {
                    String newEntityId = MapUtils.emptyIfNull(internalRecord.getNewEntityIds())
                            .get(matchContext.getInput().getTargetEntity());
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
            internalRecord.setOutput(output);

            // count newly allocated entities
            if (MapUtils.isNotEmpty(internalRecord.getNewEntityIds())) {
                for (Map.Entry<String, String> entry : internalRecord.getNewEntityIds().entrySet()) {
                    String entity = entry.getKey();
                    String entityId = entry.getValue();
                    if (StringUtils.isNotBlank(entityId)) {
                        newEntityCnt.put(entity, newEntityCnt.getOrDefault(entity, 0L) + 1);
                    }
                }
            }

            // For LDC and Entity match, IsMatched flag is marked in
            // FuzzyMatchServiceImpl.fetchIdResult
            if (internalRecord.isMatched()) {
                totalMatched++;

            }
            internalRecord.setResultsInPartition(null);

            OutputRecord outputRecord = new OutputRecord();
            if (returnUnmatched || internalRecord.isMatched()) {
                if (excludeUnmatchedPublicDomain && internalRecord.isPublicDomain()) {
                    log.warn("Excluding the record, because it is using the public domain: "
                            + internalRecord.getParsedDomain());
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
            if (matchContext.getOutput().getStatistics().getNullEntityIdCount() != null) {
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
