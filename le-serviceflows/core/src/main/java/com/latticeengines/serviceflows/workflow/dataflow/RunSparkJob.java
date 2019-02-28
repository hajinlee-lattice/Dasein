package com.latticeengines.serviceflows.workflow.dataflow;

import java.io.IOException;
import java.util.Collections;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.WorkflowSparkJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class RunSparkJob<S extends SparkJobStepConfiguration, //
        C extends WorkflowSparkJobConfig, J extends AbstractSparkJob<C>> extends BaseWorkflowStep<S> { //

    @Inject
    private SparkJobService sparkJobService;

    @Inject
    private LivySessionHolder livySessionHolder;

    @Value("${camille.zk.pod.id}")
    private String podId;

    protected CustomerSpace customerSpace;

    @Override
    public void execute() {
        log.info("Executing spark job " + getJobClz().getSimpleName());
        customerSpace = CustomerSpace.parse(getConfiguration().getCustomer());
        C jobConfig = configureJob(configuration);
        String tenantId = customerSpace.getTenantId();
        String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        jobConfig.setWorkspace(workspace);
        log.info("Run spark job " + getJobClz().getSimpleName() + " with configuration: " + JsonUtils.serialize(jobConfig));
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        SparkJobResult result = retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark job " //
                        + getJobClz().getSimpleName());
                livySessionHolder.killSession();
            }
            LivySession session = livySessionHolder //
                    .getOrCreateLivySession(tenantId + "~" + getJobClz().getSimpleName());
            return sparkJobService.runJob(session, getJobClz(), jobConfig);
        });
        postJobExecution(result);
        livySessionHolder.killSession();
    }

    protected abstract Class<J> getJobClz();

    /**
     * Set job config except jobName and workspace.
     */
    protected abstract C configureJob(S stepConfiguration);

    protected abstract void postJobExecution(SparkJobResult result);

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        String srcPath = jobTarget.getPath();
        Table table = MetadataConverter.getTable(yarnConfiguration, srcPath);
        table.setName(tableName);

        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
            HdfsUtils.moveFile(yarnConfiguration, srcPath, tgtPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + tgtPath);
        }

        Extract extract = new Extract();
        extract.setPath(tgtPath);
        if (jobTarget.getCount() != null) {
            extract.setProcessedRecords(jobTarget.getCount());
        }
        extract.setName(NamingUtils.timestamp("Extract"));
        extract.setExtractionTimestamp(System.currentTimeMillis());
        table.setExtracts(Collections.singletonList(extract));

        return table;
    }

}
