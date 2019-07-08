package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;

public abstract class PipelineTransformationTestNGBase
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private ObjectMapper om = new ObjectMapper();

    @Inject
    PipelineSource source;

    @Inject
    protected DataCloudVersionEntityMgr versionEntityMgr;

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathForResult() {
        TableSource tableSource = getTargetTableSource();
        if (tableSource != null) {
            return hdfsPathBuilder.constructTablePath(tableSource.getTable().getName(), tableSource.getCustomerSpace(),
                    tableSource.getTable().getNamespace()).toString();
        } else {
            String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(getTargetSourceName());
            return hdfsPathBuilder.constructSnapshotDir(getTargetSourceName(), targetVersion).toString();
        }
    }

    @Override
    protected String getPathToUploadBaseData() {
        return "";
    }

    protected abstract String getTargetSourceName();

    protected TableSource getTargetTableSource() {
        return null;
    }

    protected String setDataFlowEngine(String conf, String engine) {
        TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
        engineConfiguration.setEngine(engine);
        return setDataFlowEngine(conf, engineConfiguration);
    }

    protected String setDataFlowEngine(String conf,
            TransformationFlowParameters.EngineConfiguration engineConfiguration) {
        try {
            ObjectNode on = om.valueToTree(om.readTree(conf));
            on.set("EngineConfig", om.valueToTree(engineConfiguration));
            return om.writeValueAsString(on);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
