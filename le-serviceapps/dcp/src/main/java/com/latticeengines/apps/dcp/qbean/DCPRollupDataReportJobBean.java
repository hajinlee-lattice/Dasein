package com.latticeengines.apps.dcp.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.service.impl.DCPRollupDataReportJobCallable;
import com.latticeengines.apps.dcp.workflow.DCPDataReportWorkflowSubmitter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("dcpRollupDataReportJob")
public class DCPRollupDataReportJobBean implements QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(DCPRollupDataReportJobBean.class);

    @Inject
    private DataReportEntityMgr dataReportEntityMgr;

    @Inject
    private DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));

        DCPRollupDataReportJobCallable.Builder builder  = new DCPRollupDataReportJobCallable.Builder();
        builder.jobArguments(jobArguments).dataReportEntityMgr(dataReportEntityMgr)
                .dcpDataReportWorkflowSubmitter(dcpDataReportWorkflowSubmitter)
                .zkConfigService(zkConfigService).workflowProxy(workflowProxy);
        return new DCPRollupDataReportJobCallable(builder);
    }
}
