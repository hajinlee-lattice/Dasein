package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public interface TransformationDataFlowService {

    void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid, String flowBean,
            TransformationConfiguration transformationConfiguration);

}
