package com.latticeengines.propdata.workflow.engine.steps;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("propdataTransformationStepExecution")
@Scope("prototype")
public class TransformationStepExecution extends BaseWorkflowStep<PrepareTransformationStepInputConfiguration>
        implements ApplicationContextAware {
    private static final String CREATOR = "CREATOR";

    private static Log log = LogFactory.getLog(TransformationStepExecution.class);

    private TransformationService transformationService;

    private YarnClient yarnClient;

    private PrepareTransformationStepInputConfiguration prepareTransformationConfiguration;

    private ApplicationContext applicationContext;

    @Autowired
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    @Override
    public void execute() {
        TransformationProgress progress = null;
        try {
            log.info("Inside TransformationStepExecution execute()");
            initializeYarnClient();
            prepareTransformationConfiguration = getConfiguration();
            String serviceBeanName = prepareTransformationConfiguration.getServiceBeanName();
            transformationService = (TransformationService) applicationContext.getBean(serviceBeanName);
            Class<? extends TransformationConfiguration> configurationClass = transformationService
                    .getConfigurationClass();

            String transformationConfigurationStr = prepareTransformationConfiguration.getTransformationConfiguration();
            TransformationConfiguration transformationConfiguration = (TransformationConfiguration) JsonUtils
                    .deserialize(transformationConfigurationStr, configurationClass);
            if (transformationService.isNewDataAvailable(transformationConfiguration)) {
                String creator = (String) executionContext.get(CREATOR);
                String podId = prepareTransformationConfiguration.getHdfsPodId();
                HdfsPodContext.changeHdfsPodId(podId);

                progress = transformationProgressEntityMgr
                        .findProgressByRootOperationUid(transformationConfiguration.getRootOperationId());

                log.info("Processing transformation progress: " + progress.getRootOperationUID());
                if (progress != null) {
                    progress.setStartDate(new Date());
                    progress.setStatus(TransformationProgressStatus.TRANSFORMING);
                    progress = transformationProgressEntityMgr.updateProgress(progress);
                }
                transformationService.transform(progress);
                progress.setStatus(TransformationProgressStatus.FINISHED);

            } else {
                log.info("No new data available in firehose.");
            }
        } catch (Exception e) {
            if (progress != null) {
                progress.setStatus(TransformationProgressStatus.FAILED);
            }
            throw new LedpException(LedpCode.LEDP_25013, e.getMessage(), e);
        } finally {
            if (progress != null) {
                progress.setEndDate(new Date());
                transformationProgressEntityMgr.updateProgress(progress);
            }
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
