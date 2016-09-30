package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BuiltWithTechIndicators;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("builtWithTechIndicatorsService")
public class BuiltWithTechIndicatorsService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, TransformationFlowParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(BuiltWithTechIndicatorsService.class);

    @Autowired
    private BuiltWithTechIndicators source;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "builtWithTechIndicatorsFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "builtWithTechIndicatorsService";
    }

    @Override
    protected Log getLogger() {
        return log;
    }

}
