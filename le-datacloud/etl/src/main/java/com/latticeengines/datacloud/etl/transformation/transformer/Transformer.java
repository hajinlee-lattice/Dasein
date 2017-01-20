package com.latticeengines.datacloud.etl.transformation.transformer;

import java.util.List;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public interface Transformer {

    String getName();
    boolean validateConfig(String confStr, List<String> sourceNames);
    boolean transform(TransformationProgress progress, String workflowDir, Source[] baseSources, List<String> baseVersions,
                      Source[] baseTemplates, Source targetTemplate, String confStr);
}
