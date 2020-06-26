package com.latticeengines.serviceflows.workflow.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.spark.RunSparkWorkflowStepConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.utils.SparkJobClzUtils;

@Lazy
@Component("runSparkWorkflowStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RunSparkWorkflowStep extends BaseSparkStep<RunSparkWorkflowStepConfig> {

    private static final Logger log = LoggerFactory.getLogger(RunSparkWorkflowStep.class);

    @Override
    public void execute() {
        RunSparkWorkflowRequest request = configuration.getRequest();
        SparkJobConfig jobConfig = request.getSparkJobConfig();
        String jobClz = request.getSparkJobClz();
        Class<? extends AbstractSparkJob<SparkJobConfig>> sparkJob;
        try {
            sparkJob = SparkJobClzUtils.findSparkJobClz(jobClz, SparkJobConfig.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot find a spark job class named " + jobClz, e);
        }
        if (request.getPartitionMultiplier() != null) {
            setPartitionMultiplier(request.getPartitionMultiplier());
        }
        SparkJobResult result = runSparkJob(sparkJob, jobConfig);
        log.info("SparkJobResult: {}", JsonUtils.serialize(result));
        if (Boolean.TRUE.equals(request.getKeepWorkspace())) {
            log.info("Keeping random workspace: {}", jobConfig.getWorkspace());
            keepRandomWorkspace(jobConfig.getWorkspace());
        }
    }

    @Override
    protected CustomerSpace parseCustomerSpace(RunSparkWorkflowStepConfig stepConfiguration) {
        return CustomerSpace.parse(stepConfiguration.getCustomerSpace());
    }

}
