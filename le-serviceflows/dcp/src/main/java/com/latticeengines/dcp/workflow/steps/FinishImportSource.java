package com.latticeengines.dcp.workflow.steps;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    @Inject
    private UploadProxy uploadProxy;

    @Override
    public void execute() {
        Long uploadPid = configuration.getUploadPid();
        String customerSpace = configuration.getCustomerSpace().toString();
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        uploadProxy.registerMatchResult(customerSpace, uploadPid, matchResultName);
        // mark match result table as permanent
        registerTable(matchResultName);
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        long statsId = configuration.getStatsPid();
        uploadProxy.updateStatsContent(customerSpace, uploadPid, statsId, stats);
        uploadProxy.setLatestStats(customerSpace, uploadPid, statsId);
    }

}
