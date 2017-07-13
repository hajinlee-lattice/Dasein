package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class PipelineTransformationDeploymentTestNGBase extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PipelineTransformationDeploymentTestNGBase.class);

    @Autowired
    protected MetadataProxy metadataProxy;

}
