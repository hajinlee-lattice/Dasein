package com.latticeengines.propdata.workflow.engine.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("propdataTransformationProceedProgressStepExecution")
@Scope("prototype")
public class TransformationProceedProgressStepExecution
        extends BaseWorkflowStep<TransformationStepExecutionConfiguration> {
    private static final String VERSION = "VERSION";
    private static final String TRANSFORMATION_CONFIGURATION = "TRANSFORMATION_CONFIGURATION";

    private static Log log = LogFactory.getLog(TransformationProceedProgressStepExecution.class);

    @Autowired
    @Qualifier("bomboraFirehoseIngestionService")
    private TransformationService transformationService;

    private YarnClient yarnClient;

    @Override
    public void execute() {
        try {
            log.info("Inside TransformationStepExecution execute()");
            initializeYarnClient();
            if (transformationService.isNewDataAvailable(null)) {
                TransformationConfiguration transformationConfiguration = (TransformationConfiguration) executionContext
                        .get(TRANSFORMATION_CONFIGURATION);
                log.info("Processing version: " + transformationConfiguration.getSourceConfigurations().get(VERSION));
                transformationService
                        .transform(findTransformationProgressToProceed(transformationConfiguration.getSourceName()));
            } else {
                log.info("No new data available in firehose.");
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25013, e.getMessage(), e);
        }
    }

    private TransformationProgress findTransformationProgressToProceed(String sourceName) {
        return null;
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

}
