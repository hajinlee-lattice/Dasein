package com.latticeengines.propdata.workflow.engine.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("propdataTransformationStepExecution")
@Scope("prototype")
public class TransformationStepExecution extends BaseWorkflowStep<PrepareTransformationStepInputConfiguration>
        implements ApplicationContextAware {
    private static final String CREATOR = "CREATOR";

    private static Log log = LogFactory.getLog(TransformationStepExecution.class);

    private ObjectMapper om = new ObjectMapper();

    private TransformationService transformationService;

    private YarnClient yarnClient;

    private PrepareTransformationStepInputConfiguration prepareTransformationConfiguration;

    private ApplicationContext applicationContext;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Override
    public void execute() {
        try {
            log.info("Inside TransformationStepExecution execute()");
            initializeYarnClient();
            prepareTransformationConfiguration = getConfiguration();
            String serviceBeanName = prepareTransformationConfiguration.getServiceBeanName();
            transformationService = (TransformationService) applicationContext.getBean(serviceBeanName);
            Class<? extends TransformationConfiguration> configurationClass = transformationService
                    .getConfigurationClass();

            String transformationConfigurationStr = prepareTransformationConfiguration.getTransformationConfiguration();
            TransformationConfiguration transformationConfiguration = (TransformationConfiguration) om
                    .readValue(transformationConfigurationStr, configurationClass);
            if (transformationService.isNewDataAvailable(transformationConfiguration)) {
                String creator = (String) executionContext.get(CREATOR);
                String podId = prepareTransformationConfiguration.getHdfsPodId();
                hdfsPathBuilder.changeHdfsPodId(podId);
                log.info("About to create transformation progress entry for podId: " + podId);
                TransformationProgress transformConfig = transformationService
                        .startNewProgress(transformationConfiguration, creator);
                log.info("Created transformation progress entry: " + transformConfig.getRootOperationUID());
                transformationService.transform(transformConfig);
                log.info("Submitted transformation job");

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

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
