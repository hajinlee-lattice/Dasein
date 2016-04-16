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
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("propdataTransformationStepExecution")
@Scope("prototype")
public class TransformationStepExecution extends BaseWorkflowStep<TransformationStepExecutionConfiguration> {
    private static final String VERSION = "VERSION";

    private static final String TRANSFORMATION_CONFIGURATION = "TRANSFORMATION_CONFIGURATION";

    private static final String CREATOR = "CREATOR";

    private static Log log = LogFactory.getLog(TransformationStepExecution.class);

    @Autowired
    @Qualifier("bomboraFirehoseIngestionService")
    TransformationService transformationService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    private YarnClient yarnClient;
    private String rootOperationUid;

    @Override
    public void execute() {
        try {
            log.info("Inside TransformationStepExecution execute()");
            initializeYarnClient();
            if (transformationService.isNewDataAvailable()) {
                String creator = (String) executionContext.get(CREATOR);
                TransformationConfiguration transformationConfiguration = (TransformationConfiguration) executionContext
                        .get(TRANSFORMATION_CONFIGURATION);
                log.info("Processing version: " + transformationConfiguration.getSourceConfigurations().get(VERSION));
                transformationService.startNewProgress(transformationConfiguration, creator);
            } else {
                log.info("No new data available in firehose.");
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25013, e.getMessage(), e);
        }
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

}
