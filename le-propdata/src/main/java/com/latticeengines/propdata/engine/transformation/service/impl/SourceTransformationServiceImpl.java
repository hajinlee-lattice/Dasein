package com.latticeengines.propdata.engine.transformation.service.impl;

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
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.transformation.service.SourceTransformationService;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("sourceTransformationService")
public class SourceTransformationServiceImpl implements SourceTransformationService, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(SourceTransformationServiceImpl.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private WorkflowProxy workflowProxy;

    private ApplicationContext applicationContext;

    @Override
    public List<TransformationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        return scanForNewWorkFlow(hdfsPod);
    }

    @Override
    public TransformationProgress transform(String transformationName, TransformationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        TransformationService transformationService = (TransformationService) applicationContext
                .getBean(request.getSourceBeanName());
        TransformationExecutor executor = new TransformationExecutorImpl(transformationService, workflowProxy,
                hdfsPathBuilder);

        return executor.kickOffNewProgress();
    }

    private List<TransformationProgress> scanForNewWorkFlow(String hdfsPod) {
        return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
