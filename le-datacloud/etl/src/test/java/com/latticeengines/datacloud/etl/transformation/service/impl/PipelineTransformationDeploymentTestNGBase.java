package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class PipelineTransformationDeploymentTestNGBase extends PipelineTransformationTestNGBase {

    private static final Log log = LogFactory.getLog(PipelineTransformationDeploymentTestNGBase.class);

    @Autowired
    protected MetadataProxy metadataProxy;

}
