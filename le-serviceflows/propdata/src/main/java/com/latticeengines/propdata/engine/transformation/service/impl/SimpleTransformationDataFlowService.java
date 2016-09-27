package com.latticeengines.propdata.engine.transformation.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

@Component("simpleTransformationDataFlowService")
public class SimpleTransformationDataFlowService extends AbstractTransformationDataFlowService {

    public void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid,
            String flowBean, TransformationConfiguration transformationConfiguration) {
    }

}
