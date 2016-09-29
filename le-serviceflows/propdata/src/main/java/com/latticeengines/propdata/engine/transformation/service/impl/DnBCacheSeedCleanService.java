package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.DnBCacheSeed;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.DnBCacheSeedConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.DnBCacheSeedInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("dnbCacheSeedCleanService")
public class DnBCacheSeedCleanService extends SimpleTransformationServiceBase<DnBCacheSeedConfiguration, TransformationFlowParameters>
        implements TransformationService<DnBCacheSeedConfiguration> {

    private static final String DATA_FLOW_BEAN_NAME = "dnbCacheSeedCleanFlow";

    private static final Log log = LogFactory.getLog(DnBCacheSeedCleanService.class);

    @Autowired
    private DnBCacheSeed source;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    public String getDataFlowBeanName() { return DATA_FLOW_BEAN_NAME; }

    @Override
    protected TransformationFlowParameters getDataFlowParameters(TransformationProgress progress,
                                                                 DnBCacheSeedConfiguration configuration) {
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        enrichStandardDataFlowParameters(parameters, configuration, progress);
        return parameters;
    }

    @Override
    Date checkTransformationConfigurationValidity(DnBCacheSeedConfiguration conf) {
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    DnBCacheSeedConfiguration createNewConfiguration(List<String> latestBaseVersion, String newLatestVersion,
            List<SourceColumn> sourceColumns) {
        DnBCacheSeedConfiguration configuration = new DnBCacheSeedConfiguration();
        DnBCacheSeedInputSourceConfig dnbCacheSeedInputSourceConfig = new DnBCacheSeedInputSourceConfig();
        dnbCacheSeedInputSourceConfig.setVersion(latestBaseVersion.get(0));
        configuration.setDnBCacheSeedInputSourceConfig(dnbCacheSeedInputSourceConfig);
        setAdditionalDetails(newLatestVersion, sourceColumns, configuration);
        return configuration;
    }

    @Override
    DnBCacheSeedConfiguration parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, DnBCacheSeedConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return DnBCacheSeedConfiguration.class;
    }

}
