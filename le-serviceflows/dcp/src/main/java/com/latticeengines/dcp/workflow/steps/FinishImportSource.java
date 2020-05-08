package com.latticeengines.dcp.workflow.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

// use BaseSparkStep because it can export table to s3
@Component("finishImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishImportSource extends BaseSparkStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishImportSource.class);

    @Inject
    private UploadProxy uploadProxy;

    private String uploadId;
    private String customerSpaceStr;

    @Override
    public void execute() {
        uploadId = configuration.getUploadId();
        customerSpace = configuration.getCustomerSpace();
        customerSpaceStr = customerSpace.toString();
        saveMatchResultTable();
        saveMatchCandidatesTable();
        updateStats();
    }

    private void saveMatchResultTable() {
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        Table matchResultTable = metadataProxy.getTable(customerSpaceStr, matchResultName);
        exportToS3(matchResultTable);
        registerTable(matchResultName);
        uploadProxy.registerMatchResult(customerSpaceStr, uploadId, matchResultName);
    }

    private void saveMatchCandidatesTable() {
        String matchCandidatesTableName = getStringValueFromContext(MATCH_CANDIDATES_TABLE_NAME);
        if (StringUtils.isNotBlank(matchCandidatesTableName)) {
            Table matchCandidatesTable = metadataProxy.getTable(customerSpaceStr, matchCandidatesTableName);
            exportToS3(matchCandidatesTable);
            registerTable(matchCandidatesTableName);
            uploadProxy.registerMatchCandidates(customerSpaceStr, uploadId, matchCandidatesTableName);
        } else {
            log.warn("No match candidates table generate.");
        }
    }

    private void updateStats() {
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        long statsId = configuration.getStatsPid();
        uploadProxy.updateStatsContent(customerSpaceStr, uploadId, statsId, stats);
        uploadProxy.setLatestStats(customerSpaceStr, uploadId, statsId);
    }

}
