package com.latticeengines.propdata.engine.transformation.service.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraFirehose;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

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
