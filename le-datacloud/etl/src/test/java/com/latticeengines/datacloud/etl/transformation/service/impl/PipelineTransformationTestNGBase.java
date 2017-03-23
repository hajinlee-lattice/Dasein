package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public abstract class PipelineTransformationTestNGBase
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    @Autowired
    PipelineSource source;

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
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

    protected abstract String getTargetSourceName();

    protected TableSource getTargetTableSource() {
        return null;
    }

}
