package com.latticeengines.datacloud.etl.transformation.service.impl;

import javax.inject.Inject;

import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class PipelineTransformationDeploymentTestNGBase extends PipelineTransformationTestNGBase {

    @Inject
    protected MetadataProxy metadataProxy;

}
