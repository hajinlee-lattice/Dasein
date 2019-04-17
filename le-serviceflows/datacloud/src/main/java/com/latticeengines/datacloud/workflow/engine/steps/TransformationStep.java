package com.latticeengines.datacloud.workflow.engine.steps;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@SuppressWarnings("rawtypes")
@Component("transformationStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TransformationStep extends BaseWorkflowStep<PrepareTransformationStepInputConfiguration>
        implements ApplicationContextAware {
    private static Logger log = LoggerFactory.getLogger(TransformationStep.class);

    private ApplicationContext applicationContext;

    @Autowired
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        TransformationProgress progress = null;
        try {
            log.info("Inside TransformationStepExecution execute()");
            PrepareTransformationStepInputConfiguration prepareTransformationConfiguration = getConfiguration();
            String serviceBeanName = prepareTransformationConfiguration.getServiceBeanName();
            TransformationService transformationService = (TransformationService) applicationContext
                    .getBean(serviceBeanName);
            Class<? extends TransformationConfiguration> configurationClass = transformationService
                    .getConfigurationClass();

            String transformationConfigurationStr = prepareTransformationConfiguration.getTransformationConfiguration();
            TransformationConfiguration transformationConfiguration = JsonUtils
                    .deserialize(transformationConfigurationStr, configurationClass);
            if (transformationService.isNewDataAvailable(transformationConfiguration)) {
                String podId = prepareTransformationConfiguration.getHdfsPodId();
                HdfsPodContext.changeHdfsPodId(podId);

                progress = transformationProgressEntityMgr
                        .findProgressByRootOperationUid(transformationConfiguration.getRootOperationId());

                log.info("Processing transformation progress: " + progress.getRootOperationUID());
                progress.setStartDate(new Date());
                progress.setStatus(ProgressStatus.PROCESSING);
                progress = transformationProgressEntityMgr.updateProgress(progress);
                transformationService.transform(progress, transformationConfiguration);
                putStringValueInContext(TRANSFORM_PIPELINE_VERSION, progress.getVersion());
            } else {
                log.info("No new data available in firehose.");
            }
        } catch (Exception e) {
            if (progress != null) {
                progress.setStatus(ProgressStatus.FAILED);
            }
            throw new LedpException(LedpCode.LEDP_25013, e.getMessage(), e);
        } finally {
            if (progress != null) {
                progress = transformationProgressEntityMgr
                        .findProgressByRootOperationUid(progress.getRootOperationUID());
                progress.setEndDate(new Date());
                transformationProgressEntityMgr.updateProgress(progress);
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
