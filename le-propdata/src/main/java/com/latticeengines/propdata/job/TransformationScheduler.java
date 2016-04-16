package com.latticeengines.propdata.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.core.service.ServiceFlowsZkConfigService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.engine.transformation.service.impl.TransformationExecutorImpl;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class TransformationScheduler extends QuartzJobBean {
    @Autowired
    private WorkflowProxy workflowProxy;
    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;
    private TransformationService transformationService;
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;
    private boolean dryrun;
    private static final Log log = LogFactory.getLog(TransformationScheduler.class);

    private TransformationExecutor getExecutor() {
        return new TransformationExecutorImpl(transformationService, workflowProxy, hdfsPathBuilder);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (serviceFlowsZkConfigService.refreshJobEnabled(transformationService.getSource())) {
            if (dryrun) {
                log.info(transformationService.getClass().getSimpleName() + " triggered.");
            } else {
                getExecutor().kickOffNewProgress();
            }
        }
    }

    // ==============================
    // for quartz detail bean
    // ==============================
    public void setIngestionService(TransformationService ingestionService) {
        this.transformationService = ingestionService;
    }

    public void setServiceFlowsZkConfigService(ServiceFlowsZkConfigService serviceFlowsZkConfigService) {
        this.serviceFlowsZkConfigService = serviceFlowsZkConfigService;
    }

    public void setDryrun(boolean dryrun) {
        this.dryrun = dryrun;
    }

}
