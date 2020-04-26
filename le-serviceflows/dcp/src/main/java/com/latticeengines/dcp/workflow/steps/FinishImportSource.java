package com.latticeengines.dcp.workflow.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishImportSource.class);

    @Inject
    private UploadProxy uploadProxy;

    @Override
    public void execute() {
        String uploadId = configuration.getUploadId();
        String customerSpace = configuration.getCustomerSpace().toString();
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        uploadProxy.registerMatchResult(customerSpace, uploadId, matchResultName);
        String matchCandidatesTableName = getStringValueFromContext(MATCH_CANDIDATES_TABLE_NAME);
        if (StringUtils.isNotBlank(matchCandidatesTableName)) {
            uploadProxy.registerMatchResult(customerSpace, uploadId, matchCandidatesTableName);
        } else {
            log.warn("No match candidates table generate.");
        }

        // mark match result table as permanent
        registerTable(matchResultName);
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        long statsId = configuration.getStatsPid();
        uploadProxy.updateStatsContent(customerSpace, uploadId, statsId, stats);
        uploadProxy.setLatestStats(customerSpace, uploadId, statsId);
        uploadProxy.updateUploadStatus(customerSpace, uploadId, Upload.Status.FINISHED);
    }

}
