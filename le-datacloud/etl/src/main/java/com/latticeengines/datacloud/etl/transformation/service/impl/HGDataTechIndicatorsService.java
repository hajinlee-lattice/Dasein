package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.HGDataTechIndicators;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

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
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "hgDataTechIndicatorsFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "hgDataTechIndicatorsService";
    }

}
