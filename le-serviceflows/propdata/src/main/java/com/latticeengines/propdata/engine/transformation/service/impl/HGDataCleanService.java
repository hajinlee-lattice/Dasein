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
import com.latticeengines.propdata.core.source.impl.HGDataClean;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.HGDataCleanConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

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
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return HGDataCleanConfiguration.class;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() { return "hgDataCleanFlow"; }

    @Override
    HGDataCleanConfiguration createNewConfiguration(List<String> latestBaseVersions, String newLatestVersion,
            List<SourceColumn> sourceColumns) {
        HGDataCleanConfiguration configuration = new HGDataCleanConfiguration();
        configuration.setSourceName(hgDataClean.getSourceName());
        configuration.setSourceName("hgDataCleanService");
        configuration.setVersion(newLatestVersion);
        return configuration;
    }

    @Override
    protected TransformationFlowParameters getDataFlowParameters(TransformationProgress progress,
                                                                 HGDataCleanConfiguration transformationConfiguration) {
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        enrichStandardDataFlowParameters(parameters, transformationConfiguration, progress);
        Date fakedNow = transformationConfiguration.getFakedCurrentDate();
        if (fakedNow != null) {
            parameters.setTimestamp(fakedNow);
        }
        return parameters;
    }
}
