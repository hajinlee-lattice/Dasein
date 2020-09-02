package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.ANALYSIS_PERCENTAGE;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

// use BaseSparkStep because it can export table to s3
@Component("finishImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishImportSource extends BaseSparkStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishImportSource.class);

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    private String uploadId;
    private String customerSpaceStr;

    @Override
    public void execute() {
        uploadId = configuration.getUploadId();
        customerSpace = configuration.getCustomerSpace();
        customerSpaceStr = customerSpace.toString();
        saveMatchResultTable();
        saveDunsCountTable();
        updateStats();
        uploadProxy.updateProgressPercentage(customerSpaceStr, uploadId, ANALYSIS_PERCENTAGE);
    }

    private void saveMatchResultTable() {
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        Table matchResultTable = metadataProxy.getTable(customerSpaceStr, matchResultName);
        exportToS3(matchResultTable);
        registerTable(matchResultName);
        uploadProxy.registerMatchResult(customerSpaceStr, uploadId, matchResultName);
    }

    private void saveDunsCountTable() {
        String dunsCountTableName = getStringValueFromContext(DUNS_COUNT_TABLE_NAME);
        Table dunsCountTable = metadataProxy.getTable(customerSpaceStr, dunsCountTableName);
        exportToS3(dunsCountTable);
        registerTable(dunsCountTableName);
    }

    private void updateStats() {
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        long statsId = configuration.getStatsPid();
        uploadProxy.updateStatsContent(customerSpaceStr, uploadId, statsId, stats);
        uploadProxy.setLatestStats(customerSpaceStr, uploadId, statsId);

        DataReport.BasicStats basicStats = new DataReport.BasicStats();
        basicStats.setTotalSubmitted(stats.getImportStats().getSubmitted());
        basicStats.setSuccessCnt(stats.getImportStats().getSuccessfullyIngested());
        basicStats.setErrorCnt(stats.getImportStats().getFailedIngested());
        basicStats.setMatchedCnt(stats.getMatchStats().getMatched());
        basicStats.setUnmatchedCnt(stats.getMatchStats().getUnmatched());
        basicStats.setPendingReviewCnt(stats.getMatchStats().getPendingReviewCnt());

        dataReportProxy.updateDataReport(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                uploadId, basicStats);

        DataReport.InputPresenceReport inputPresenceReport = getObjectFromContext(INPUT_PRESENCE_REPORT, DataReport.InputPresenceReport.class);
        dataReportProxy.updateDataReport(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                configuration.getUploadId(), inputPresenceReport);

        dataReportProxy.copyDataReportToParent(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload, configuration.getUploadId());
    }

}
