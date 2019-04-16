package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraFirehose;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.BomboraFirehoseIngestionService;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BomboraFirehoseConfiguration;

public class BomboraFirehoseDeploymentTestNG extends FirehoseTransformationDeploymentTestNGBase<BomboraFirehoseConfiguration> {

    @Autowired
    BomboraFirehoseIngestionService refreshService;

    @Autowired
    BomboraFirehose source;

    @Override
    protected TransformationService<BomboraFirehoseConfiguration> getTransformationService() {
        return refreshService;
    }

    @Override
    protected String getTransformationServiceBeanName() { return "bomboraFirehoseIngestionService"; }

    @Override
    protected Source getSource() {
        return source;
    }

}
