package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.propdata.core.source.Source;

public interface TransformationDataFlowService {

    void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid, String flowBean);

}
