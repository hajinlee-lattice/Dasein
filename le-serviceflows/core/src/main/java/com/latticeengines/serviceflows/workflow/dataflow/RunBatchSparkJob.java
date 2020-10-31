package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public abstract class RunBatchSparkJob<S extends BaseStepConfiguration, C extends SparkJobConfig> //
        extends RunSparkJob<S, C> { //

    private static final Logger log = LoggerFactory.getLogger(RunBatchSparkJob.class);

    protected abstract List<C> batchConfigs(S jobConfig);

    protected abstract void postJobExecutions(List<SparkJobResult> results);

    @Override
    protected C configureJob(S stepConfiguration) {
        return null;
    }

    @Override
    public void execute() {
        log.info("Executing spark job " + getJobClz().getSimpleName());
        customerSpace = parseCustomerSpace(configuration);
        List<C> jobConfigs = batchConfigs(configuration);
        if (CollectionUtils.isNotEmpty(jobConfigs)) {
            List<SparkJobResult> results = new ArrayList<>();
            for (C config : jobConfigs) {
                SparkJobResult result = runSparkJob(getJobClz(), config);
                results.add(result);
            }
            postJobExecutions(results);
        } else {
            log.info("Spark job config is null, skip submitting spark job.");
        }
    }

}
