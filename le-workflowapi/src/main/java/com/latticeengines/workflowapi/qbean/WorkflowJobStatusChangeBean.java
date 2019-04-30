package com.latticeengines.workflowapi.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflowapi.service.impl.WorkflowJobStatusChangeCallable;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("workflowJobStatusChangeJob")
public class WorkflowJobStatusChangeBean implements QuartzJobBean {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobStatusChangeBean.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private JobCacheService jobCacheService;

    @Inject
    private EMRService emrService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));

        WorkflowJobStatusChangeCallable.Builder builder = new WorkflowJobStatusChangeCallable.Builder();
        builder.workflowJobEntityMgr(workflowJobEntityMgr).emrEnvService(emrEnvService).emrService(emrService)
                .jobCacheService(jobCacheService);
        return new WorkflowJobStatusChangeCallable(builder);
    }

}
