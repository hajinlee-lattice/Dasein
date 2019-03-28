package com.latticeengines.datacloud.etl.transformation.transformer;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public interface Transformer {

    String getName();

    boolean validateConfig(String confStr, List<String> sourceNames);

    boolean transform(TransformationProgress pipelineProgress, String workflowDir, TransformStep step);

    void initBaseSources(String confStr, List<String> sourceNames);

    String outputSubDir();
}
