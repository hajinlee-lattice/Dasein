package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.core.spark.WorkflowSparkJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;

public abstract class BaseCoreSparkJobStep<S extends SparkJobStepConfiguration, C extends WorkflowSparkJobConfig,
        J extends AbstractSparkJob<C>> extends RunSparkJob<S, C, J> {

    @Override
    protected List<String> getSwLibs() {
        return Collections.singletonList("scoring");
    }

}
