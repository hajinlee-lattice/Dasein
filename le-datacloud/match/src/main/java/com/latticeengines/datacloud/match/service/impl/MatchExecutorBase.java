package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.DisposableEmailService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.datafabric.entitymanager.GenericFabricMessageManager;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
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

    @Value("${datacloud.match.publish.match.history:false}")
    private boolean isMatchHistoryEnabled;

    @Resource(name = "genericFabricMessageManager")
    private GenericFabricMessageManager<MatchHistory> fabricEntityManager;

    @PostConstruct
    public void init() {
        if (isMatchHistoryEnabled) {
            fabricEntityManager.createOrGetNamedBatchId(FABRIC_MATCH_HISTORY, null, false);
        }
    }

    @MatchStep
    protected MatchContext complete(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        matchContext = updateInternalResults(dbHelper, matchContext);
        matchContext = mergeResults(matchContext);
        matchContext.getOutput().setFinishedAt(new Date());
        Long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
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
                    .setRawEmail(record.getOrigEmail()).withRawNameLocation(record.getOrigNameLocation());
            matchHistory.setStandardisedDomain(record.getParsedDomain()).setStandardisedDUNS(record.getParsedDuns())
                    .setStandardisedEmail(record.getParsedEmail())
                    .withStandardisedNameLocation(record.getParsedNameLocation());
            matchHistory.setIsPublicDomain(record.isPublicDomain()).setLatticeAccountId(record.getLatticeAccountId());

            matchHistory.setMatchedDomain(record.getMatchedDomain()).setMatchedEmail(record.getMatchedEmail())
                    .setMatchedDUNS(record.getMatchedDuns()).withMatchedNameLocation(record.getMatchedNameLocation());
            matchHistory.setMatchedEmployeeRange(record.getMatchedEmployeeRange())
                    .setMatchedRevenueRange(record.getMatchedRevenueRange())
                    .setMatchedPrimaryIndustry(record.getMatchedPrimaryIndustry())
                    .setMatchedSecondaryIndustry(record.getMatchedSecondIndustry())
                    .setDomainSource(record.getDomainSource())
                    .setRequestTimestamp(DateTimeUtils.format(record.getRequestTimeStamp()));

            MatchInput matchInput = matchContext.getInput();
            if (matchInput != null) {
                if (matchInput.getTenant() != null) {
                    matchHistory.setTenantId(matchInput.getTenant().getId()).setRootOperationUid(
                            matchInput.getRootOperationUid());
                }
                if (matchInput.getRequestSource() != null)
                    matchHistory.setRequestSource(matchInput.getRequestSource().toString());
            }

            matchHistories.add(matchHistory);
        }

        publishMatchHistory(matchHistories);
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

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    @MatchStep
    MatchContext mergeResults(MatchContext matchContext) {
        ColumnSelectionService columnSelectionService = beanDispatcher.getColumnSelectionService(matchContext);
        MetadataColumnService<MetadataColumn> metadataColumnService = beanDispatcher
                .getMetadataColumnService(matchContext);

        List<InternalOutputRecord> records = matchContext.getInternalResults();
        List<String> columnNames = columnSelectionService.getMatchedColumns(matchContext.getColumnSelection());
        List<Column> columns = matchContext.getColumnSelection().getColumns();
        boolean returnUnmatched = matchContext.isReturnUnmatched();
        boolean excludeUnmatchedPublicDomain = Boolean.TRUE.equals(matchContext.getInput()
                .getExcludePublicDomain());

        List<OutputRecord> outputRecords = new ArrayList<>();
        Integer[] columnMatchCount = new Integer[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnMatchCount[i] = 0;
        }

        int totalMatched = 0;

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

            List<MetadataColumn> metadataColumns = metadataColumnService.getMetadataColumns(columnNames, matchContext
                    .getInput().getDataCloudVersion());
            Map<String, MetadataColumn> metadataColumnMap = new HashMap<>();
            for (MetadataColumn column : metadataColumns) {
                metadataColumnMap.put(column.getColumnId(), column);
            }

            boolean matchedRecord = internalRecord.isMatched() || (internalRecord.getLatticeAccountId() != null);

            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);

                MetadataColumn metadataColumn = metadataColumnMap.get(column.getExternalColumnId());
                String field = (metadataColumn != null) ? metadataColumn.getColumnId() : column.getColumnName();

                Object value = null;

                if (MatchConstants.LID_FIELD.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getLatticeAccountId())) {
                    value = internalRecord.getLatticeAccountId();
                } else if (MatchConstants.IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                        && publicDomainService.isPublicDomain(internalRecord.getParsedDomain())) {
                    value = true;
                } else if (MatchConstants.DISPOSABLE_EMAIL.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                        && disposableEmailService.isDisposableEmailDomain(internalRecord.getParsedDomain())) {
                    value = true;
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

            internalRecord.setOutput(output);

            if (matchedRecord) {
                totalMatched++;
                internalRecord.setMatched(true);
            } else {
                internalRecord.setMatched(false);
                internalRecord.addErrorMessages("Cannot find a match in data cloud for the input.");
            }

            internalRecord.setResultsInPartition(null);
            OutputRecord outputRecord = new OutputRecord();
            if (returnUnmatched || matchedRecord) {
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
            outputRecord.setMatchedEmail(internalRecord.getMatchedEmail());
            outputRecord.setMatchedLatticeAccountId(internalRecord.getLatticeAccountId());

            outputRecord.setRowNumber(internalRecord.getRowNumber());
            outputRecord.setErrorMessages(internalRecord.getErrorMessages());
            outputRecord.setMatchLogs(internalRecord.getMatchLogs());
            outputRecord.setDebugValues(internalRecord.getDebugValues());
            outputRecord.setNumFeatureValue(internalRecord.getNumFeatureValue());
            outputRecords.add(outputRecord);
        }

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        if (columnMatchCount.length <= 10000) {
            matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));
        }

        return matchContext;
    }

}
