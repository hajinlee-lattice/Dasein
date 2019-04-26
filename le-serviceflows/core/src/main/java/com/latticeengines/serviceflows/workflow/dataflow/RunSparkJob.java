package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.SparkJobService;

public abstract class RunSparkJob<S extends BaseStepConfiguration, //
        C extends SparkJobConfig, J extends AbstractSparkJob<C>> extends BaseSparkStep<S> { //

    @Inject
    private SparkJobService sparkJobService;

    protected abstract Class<J> getJobClz();
    /**
     * Set job config except jobName and workspace.
     */
    protected abstract C configureJob(S stepConfiguration);
    protected abstract void postJobExecution(SparkJobResult result);

    @Override
    public void execute() {
        log.info("Executing spark job " + getJobClz().getSimpleName());
        customerSpace = parseCustomerSpace(configuration);
        C jobConfig = configureJob(configuration);
        if (jobConfig != null) {
            String tenantId = customerSpace.getTenantId();
            String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
            jobConfig.setWorkspace(workspace);
            log.info("Run spark job " + getJobClz().getSimpleName() + " with configuration: " + JsonUtils.serialize(jobConfig));
            computeScalingMultiplier(jobConfig.getInput());
            try {
                RetryTemplate retry = RetryUtils.getRetryTemplate(3);
                SparkJobResult result = retry.execute(context -> {
                    if (context.getRetryCount() > 0) {
                        log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark job " //
                                + getJobClz().getSimpleName());
                        log.warn("Previous failure: " + context.getLastThrowable());
                        killLivySession();
                    }
                    String jobName = tenantId + "~" + getJobClz().getSimpleName() + "~" + getClass().getSimpleName();
                    LivySession session = createLivySession(jobName);
                    return sparkJobService.runJob(session, getJobClz(), jobConfig);
                });
                postJobExecution(result);
            } finally {
                killLivySession();
            }
        } else {
            log.info("Spark job config is null, skip submitting spark job.");
        }
    }

    protected CustomerSpace parseCustomerSpace(S stepConfiguration) {
        if (stepConfiguration instanceof SparkJobStepConfiguration) {
            SparkJobStepConfiguration sparkJobStepConfiguration = (SparkJobStepConfiguration) stepConfiguration;
            return CustomerSpace.parse(sparkJobStepConfiguration.getCustomer());
        } else {
            throw new UnsupportedOperationException("Do not know how to parse customer space from a " //
                    + stepConfiguration.getClass().getCanonicalName());
        }
    }

    protected void overlayTableSchema(Table resultTable, Map<String, Attribute> attributeMap) {
        List<Attribute> attrs = resultTable.getAttributes();
        List<Attribute> newAttrs = attrs.stream().map(attr -> {
            String attrName = attr.getName();
            return attributeMap.getOrDefault(attrName, attr);
        }).collect(Collectors.toList());
        resultTable.setAttributes(newAttrs);
    }

}
