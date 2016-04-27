package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.transformation.TransformationRequest;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.SourceTransformationService;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
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

    private ApplicationContext applicationContext;

    @Override
    public List<TransformationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        return scanForNewWorkFlow(hdfsPod);
    }

    @Override
    public TransformationProgress transform(TransformationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        TransformationService transformationService = (TransformationService) applicationContext
                .getBean(request.getSourceBeanName());
        TransformationExecutor executor = new TransformationExecutorImpl(transformationService, workflowProxy);

        return executor.kickOffNewProgress(transformationProgressEntityMgr);
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
            TransformationProgress progress = transform(request, hdfsPod);
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
