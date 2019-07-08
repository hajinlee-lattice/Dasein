package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.TABLES_GOING_TO_DYNAMO;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.TABLES_GOING_TO_REDSHIFT;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.MatchEntityWorkflow;
import com.latticeengines.cdl.workflow.ProcessAccountWorkflow;
import com.latticeengines.cdl.workflow.ProcessContactWorkflow;
import com.latticeengines.cdl.workflow.ProcessProductWorkflow;
import com.latticeengines.cdl.workflow.ProcessRatingWorkflow;
import com.latticeengines.cdl.workflow.ProcessTransactionWorkflow;
import com.latticeengines.cdl.workflow.steps.process.ApsGeneration;
import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ApsGenerationStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.serviceflows.workflow.export.ExportToRedshift;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessAnalyzeChoreographer extends BaseChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeChoreographer.class);

    @Inject
    private ProcessMatchEntityChoreographer matchEntityChoreographer;

    @Inject
    private ProcessAccountChoreographer accountChoreographer;

    @Inject
    private ProcessContactChoreographer contactChoreographer;

    @Inject
    private ProcessProductChoreographer productChoreographer;

    @Inject
    private ProcessTransactionChoreographer transactionChoreographer;

    @Inject
    private ProcessRatingChoreographer ratingChoreographer;

    @Inject
    private MatchEntityWorkflow matchEntityWorkflow;

    @Inject
    private ProcessAccountWorkflow accountWorkflow;

    @Inject
    private ProcessContactWorkflow contactWorkflow;

    @Inject
    private ProcessProductWorkflow productWorkflow;

    @Inject
    private ProcessTransactionWorkflow transactionWorkflow;

    @Inject
    private ProcessRatingWorkflow ratingWorkflow;

    @Inject
    private AwsApsGeneratorStep awsApsGeneratorStep;

    @Inject
    private ApsGeneration apsGeneration;

    @Inject
    private ExportToRedshift exportToRedshiftStep;

    @Inject
    private ExportToDynamo exportToDynamoStep;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private YarnConfiguration yarnConfiguration;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        boolean skip = false;
        if (isMatchEntityStep(seq)) {
            skip = matchEntityChoreographer.skipStep(step, seq);
        } else if (isAccountStep(seq)) {
            skip = accountChoreographer.skipStep(step, seq);
        } else if (isContactStep(seq)) {
            skip = contactChoreographer.skipStep(step, seq);
        } else if (isProductStep(seq)) {
            skip = productChoreographer.skipStep(step, seq);
        } else if (isTransactionStep(seq)) {
            skip = transactionChoreographer.skipStep(step, seq);
        } else if (isApsGenerationStep(step)) {
            skip = skipApsGeneration(step);
        } else if (isExportToRedshiftStep(step)) {
            skip = skipExportToRedshiftStep(step);
        } else if (isExportToDynamoStep(step)) {
            skip = skipExportToDynamoStep(step);
        } else if (isRatingStep(seq)) {
            skip = ratingChoreographer.skipStep(step, seq);
        }
        return super.skipStep(step, seq) || skip;
    }

    @Override
    public void linkStepNamespaces(List<String> stepNamespaces) {
        super.linkStepNamespaces(stepNamespaces);
        matchEntityChoreographer.linkStepNamespaces(stepNamespaces);
        accountChoreographer.linkStepNamespaces(stepNamespaces);
        contactChoreographer.linkStepNamespaces(stepNamespaces);
        productChoreographer.linkStepNamespaces(stepNamespaces);
        transactionChoreographer.linkStepNamespaces(stepNamespaces);
        ratingChoreographer.linkStepNamespaces(stepNamespaces);
    }

    private boolean isMatchEntityStep(int seq) {
        return inWorkflow(seq, matchEntityWorkflow);
    }

    private boolean isAccountStep(int seq) {
        return inWorkflow(seq, accountWorkflow);
    }

    private boolean isContactStep(int seq) {
        return inWorkflow(seq, contactWorkflow);
    }

    private boolean isProductStep(int seq) {
        return inWorkflow(seq, productWorkflow);
    }

    private boolean isTransactionStep(int seq) {
        return inWorkflow(seq, transactionWorkflow);
    }

    private boolean isRatingStep(int seq) {
        return inWorkflow(seq, ratingWorkflow);
    }

    private boolean inWorkflow(int seq, AbstractWorkflow<?> workflow) {
        String namespace = getStepNamespace(seq);
        return workflow.name().equals(namespace) || namespace.startsWith(workflow.name() + ".")
                || namespace.contains("." + workflow.name() + ".") || namespace.endsWith("." + workflow.name());
    }

    private boolean isExportToRedshiftStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(exportToRedshiftStep.name());
    }

    private boolean skipExportToRedshiftStep(AbstractStep<? extends BaseStepConfiguration> step) {
        List<RedshiftExportConfig> exportConfigs = step.getListObjectFromContext(TABLES_GOING_TO_REDSHIFT,
                RedshiftExportConfig.class);
        return CollectionUtils.isEmpty(exportConfigs);
    }

    private boolean isExportToDynamoStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(exportToDynamoStep.name());
    }

    private boolean skipExportToDynamoStep(AbstractStep<? extends BaseStepConfiguration> step) {
        List<DynamoExportConfig> exportConfigs = step.getListObjectFromContext(TABLES_GOING_TO_DYNAMO,
                DynamoExportConfig.class);
        return CollectionUtils.isEmpty(exportConfigs);
    }

    private boolean isApsGenerationStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(awsApsGeneratorStep.name()) || step.name().equals(apsGeneration.name());
    }

    private boolean skipApsGeneration(AbstractStep<? extends BaseStepConfiguration> step) {
        boolean skip = true;
        if ((transactionChoreographer.update || transactionChoreographer.rebuild || rebuildAps(step))
                && hasAnalyticProduct(step)) {
            skip = false;
        } else {
            log.warn(
                    "Skip APS generation because there is no change in Transaction data, has no analytic product or no enforced rebuild.");
        }
        return skip;
    }

    private boolean hasAnalyticProduct(AbstractStep<? extends BaseStepConfiguration> step) {
        return transactionChoreographer.hasAnalyticProduct(step, false);
    }

    private boolean rebuildAps(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step.getConfiguration() instanceof AWSPythonBatchConfiguration) {
            AWSPythonBatchConfiguration configuration = (AWSPythonBatchConfiguration) step.getConfiguration();
            if (CollectionUtils.isNotEmpty(configuration.getRebuildSteps())) {
                return configuration.getRebuildSteps().contains(step.name().toLowerCase());
            }
        } else if (step.getConfiguration() instanceof ApsGenerationStepConfiguration) {
            ApsGenerationStepConfiguration configuration = (ApsGenerationStepConfiguration) step.getConfiguration();
            return Boolean.TRUE.equals(configuration.getForceRebuild());
        }
        return false;
    }

}
