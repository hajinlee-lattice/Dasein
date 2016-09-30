package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BuiltWithTechIndicators;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
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
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "builtWithTechIndicatorsFlow";
    }

    @Override
    protected BasicTransformationConfiguration createNewConfiguration(List<String> latestBaseVersions,
            String newLatestVersion, List<SourceColumn> sourceColumns) {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setSourceName(source.getSourceName());
        configuration.setServiceBeanName("builtWithTechIndicatorsService");
        configuration.setVersion(newLatestVersion);
        return configuration;
    }

    @Override
    protected TransformationFlowParameters getDataFlowParameters(TransformationProgress progress,
                                                                 BasicTransformationConfiguration transConf) {
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        enrichStandardDataFlowParameters(parameters, transConf, progress);
        parameters.setTimestamp(new Date());
        return parameters;
    }

}
