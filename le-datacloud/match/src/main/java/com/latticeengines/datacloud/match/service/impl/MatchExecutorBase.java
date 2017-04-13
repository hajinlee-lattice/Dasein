package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.DisposableEmailService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.newrelic.api.agent.Trace;

public abstract class MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(MatchExecutorBase.class);

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DisposableEmailService disposableEmailService;

    @Autowired
    protected MetricService metricService;

    @MatchStep
    protected MatchContext complete(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        matchContext = updateInternalResults(dbHelper, matchContext);
        matchContext = mergeResults(matchContext);
        matchContext.getOutput().setFinishedAt(new Date());
        Long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - receiveTime);
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

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    @MatchStep
    @Trace
    MatchContext mergeResults(MatchContext matchContext) {
        ColumnSelectionService columnSelectionService = beanDispatcher.getColumnSelectionService(matchContext);
        MetadataColumnService<MetadataColumn> metadataColumnService = beanDispatcher
                .getMetadataColumnService(matchContext);

        List<InternalOutputRecord> records = matchContext.getInternalResults();
        List<String> columnNames = columnSelectionService.getMatchedColumns(matchContext.getColumnSelection());
        List<Column> columns = matchContext.getColumnSelection().getColumns();
        boolean returnUnmatched = matchContext.isReturnUnmatched();
        boolean excludeUnmatchedPublicDomain = Boolean.TRUE
                .equals(matchContext.getInput().getExcludeUnmatchedWithPublicDomain());

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

            List<MetadataColumn> metadataColumns = metadataColumnService.getMetadataColumns(columnNames,
                    matchContext.getInput().getDataCloudVersion());
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
                if (excludeUnmatchedPublicDomain && !matchedRecord && internalRecord.isPublicDomain()) {
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
            outputRecord.setDnbCacheIds(internalRecord.getDnbCacheIds());
            outputRecord.setMatchedEmail(internalRecord.getMatchedEmail());
            outputRecord.setMatchedLatticeAccountId(internalRecord.getLatticeAccountId());

            outputRecord.setRowNumber(internalRecord.getRowNumber());
            outputRecord.setErrorMessages(internalRecord.getErrorMessages());
            outputRecord.setMatchLogs(internalRecord.getMatchLogs());
            outputRecord.setDebugValues(internalRecord.getDebugValues());
            outputRecords.add(outputRecord);
            log.info("output record: " + JsonUtils.serialize(outputRecord));
        }

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));

        return matchContext;
    }

}
