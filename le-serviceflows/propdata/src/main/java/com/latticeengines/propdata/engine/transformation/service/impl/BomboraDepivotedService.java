package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraDepivoted;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraDepivotConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("bomboraDepivotedService")
public class BomboraDepivotedService extends AbstractFixedIntervalTransformationService
        implements TransformationService {

    private static final String VERSION = "VERSION";

    private static final Log log = LogFactory.getLog(BomboraDepivotedService.class);

    @Autowired
    private TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private BomboraDepivoted source;

    @Autowired
    private FixedIntervalTransformationDataFlowService transformationDataFlowService;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    TransformationProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Log getLogger() {
        return log;
    }

    @Override
    protected void executeDataFlow(TransformationProgress progress, String workflowDir) {
        transformationDataFlowService.executeDataProcessing(source, workflowDir, getVersion(progress),
                progress.getRootOperationUID(), "bomboraDepivotFlow");
    }

    @Override
    protected TransformationDataFlowService getTransformationDataFlowService() {
        return transformationDataFlowService;
    }

    @Override
    Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration) {
        BomboraDepivotConfiguration conf = (BomboraDepivotConfiguration) transformationConfiguration;
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    TransformationConfiguration createNewConfiguration(String latestBaseVersion, String newLatestVersion) {
        BomboraDepivotConfiguration configuration = new BomboraDepivotConfiguration();
        BomboraFirehoseInputSourceConfig bomboraFirehoseInputSourceConfig = new BomboraFirehoseInputSourceConfig();
        bomboraFirehoseInputSourceConfig.setVersion(latestBaseVersion);
        configuration.setBomboraFirehoseInputSourceConfig(bomboraFirehoseInputSourceConfig);
        configuration.setSourceName(source.getSourceName());
        Map<String, String> sourceConfigurations = new HashMap<>();
        configuration.setSourceConfigurations(sourceConfigurations);
        configuration.setVersion(newLatestVersion);
        return configuration;
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException {
        return om.readValue(confStr, BomboraDepivotConfiguration.class);
    }
}
