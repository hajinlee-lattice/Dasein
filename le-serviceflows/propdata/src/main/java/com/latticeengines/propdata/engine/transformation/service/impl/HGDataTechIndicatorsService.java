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
import com.latticeengines.propdata.core.source.impl.HGDataTechIndicators;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("hgDataTechIndicatorsService")
public class HGDataTechIndicatorsService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, TransformationFlowParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(HGDataTechIndicatorsService.class);

    @Autowired
    private HGDataTechIndicators source;

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
        return "hgDataTechIndicatorsFlow";
    }

    @Override
    protected BasicTransformationConfiguration createNewConfiguration(List<String> latestBaseVersions,
            String newLatestVersion, List<SourceColumn> sourceColumns) {
        BasicTransformationConfiguration configuration = new BasicTransformationConfiguration();
        configuration.setSourceName(source.getSourceName());
        configuration.setSourceName("hgDataTechIndicatorsService");
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
