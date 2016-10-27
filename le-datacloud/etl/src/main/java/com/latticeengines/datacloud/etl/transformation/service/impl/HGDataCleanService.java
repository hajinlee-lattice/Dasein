package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.HGDataClean;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.HGDataCleanConfiguration;

@Component("hgDataCleanService")
public class HGDataCleanService extends SimpleTransformationServiceBase<HGDataCleanConfiguration, TransformationFlowParameters>
        implements TransformationService<HGDataCleanConfiguration> {

    private static final Log log = LogFactory.getLog(HGDataCleanService.class);

    @Autowired
    private HGDataClean hgDataClean;

    @Override
    public Source getSource() {
        return hgDataClean;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() { return "hgDataCleanFlow"; }

    @Override
    protected String getServiceBeanName() {
        return "hgDataCleanService";
    }

    @Override
    public Class<HGDataCleanConfiguration> getConfigurationClass() {
        return HGDataCleanConfiguration.class;
    }

    @Override
    protected TransformationFlowParameters getDataFlowParameters(TransformationProgress progress,
                                                                 HGDataCleanConfiguration transformationConfiguration) {
        TransformationFlowParameters parameters = super.getDataFlowParameters(progress, transformationConfiguration);
        Date fakedNow = transformationConfiguration.getFakedCurrentDate();
        if (fakedNow != null) {
            parameters.setTimestamp(fakedNow);
        }
        return parameters;
    }
}
