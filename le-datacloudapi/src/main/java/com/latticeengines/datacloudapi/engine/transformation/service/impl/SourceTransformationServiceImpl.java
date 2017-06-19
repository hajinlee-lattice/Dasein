package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DataImportedFromHDFS;
import com.latticeengines.datacloud.core.source.FixedIntervalSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.datacloudapi.engine.transformation.service.TransformationExecutor;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("sourceTransformationService")
public class SourceTransformationServiceImpl implements SourceTransformationService, ApplicationContextAware {

    private static final String PROPDATA_TRIGGER = "PROPDATA_TRIGGER";

    private static final Log log = LogFactory.getLog(SourceTransformationServiceImpl.class);

    @Autowired
    private List<FixedIntervalSource> fixedIntervalSources;

    @Autowired
    private List<DataImportedFromHDFS> hdfsImportedSources;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    @Value("${datacloud.etl.workflow.mem.mb}")
    private Integer workflowMem;

    private ApplicationContext applicationContext;

    @Override
    public List<TransformationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        return scanForNewWorkFlow(hdfsPod);
    }

    @Override
    public TransformationProgress transform(TransformationRequest request, String hdfsPod, boolean fromScan) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        TransformationService<?> transformationService = (TransformationService<?>) applicationContext.getBean(request
                .getSourceBeanName());
        if (fromScan && transformationService.isManualTriggerred()) {
            return null;
        }
        TransformationExecutor executor = new TransformationExecutorImpl(transformationService, workflowProxy);
        return executor.kickOffNewProgress(transformationProgressEntityMgr, request.getBaseVersions(),
                request.getTargetVersion());
    }

    @Override
    public TransformationProgress pipelineTransform(PipelineTransformationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        if (request.getContainerMemMB() == null) {
            request.setContainerMemMB(workflowMem);
        }

        TransformationService<?> transformationService = (TransformationService<?>) applicationContext
                .getBean("pipelineTransformationService");

        TransformationExecutor executor = new TransformationExecutorImpl(transformationService, workflowProxy);
        return executor.kickOffNewPipelineProgress(transformationProgressEntityMgr, request);
    }

    @Override
    public TransformationWorkflowConfiguration generatePipelineWorkflowConf(PipelineTransformationRequest request,
            String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        if (request.getContainerMemMB() == null) {
            request.setContainerMemMB(workflowMem);
        }
        TransformationService<?> transformationService = (TransformationService<?>) applicationContext
                .getBean("pipelineTransformationService");
        TransformationExecutor executor = new TransformationExecutorImpl(transformationService, workflowProxy);
        return executor.generateNewPipelineWorkflowConf(request);
    }

    @Override
    public TransformationProgress getProgress(String rootOperationUid) {
        return transformationProgressEntityMgr.findProgressByRootOperationUid(rootOperationUid);
    }

    private List<TransformationProgress> scanForNewWorkFlow(String hdfsPod) {
        List<TransformationProgress> progresses = new ArrayList<>();
        for (DataImportedFromHDFS source : hdfsImportedSources) {
            String transformServiceBeanName = source.getTransformationServiceBeanName();
            scanAndSubmitTransform(hdfsPod, progresses, source, transformServiceBeanName);
        }
        for (FixedIntervalSource source : fixedIntervalSources) {
            String transformServiceBeanName = source.getTransformationServiceBeanName();
            scanAndSubmitTransform(hdfsPod, progresses, source, transformServiceBeanName);
        }
        return progresses;
    }

    private void scanAndSubmitTransform(String hdfsPod, List<TransformationProgress> progresses, Source source,
            String transformServiceBeanName) {
        try {
            TransformationRequest request = new TransformationRequest();
            request.setSourceBeanName(transformServiceBeanName);
            request.setSubmitter(PROPDATA_TRIGGER);
            TransformationProgress progress = transform(request, hdfsPod, true);
            if (progress != null) {
                progresses.add(progress);
            }
        } catch (Exception e) {
            log.error("Could not start transformation for source: " + source.getSourceName(), e);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
