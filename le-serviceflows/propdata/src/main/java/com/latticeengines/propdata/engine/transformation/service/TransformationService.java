package com.latticeengines.propdata.engine.transformation.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public interface TransformationService {

    TransformationProgress startNewProgress(TransformationConfiguration transformationConfiguration, String creator);

    TransformationProgress transform(TransformationProgress progress,
            TransformationConfiguration transformationConfiguration);

    TransformationProgress finish(TransformationProgress progress);

    String getVersionString(TransformationProgress progress);

    boolean isNewDataAvailable(TransformationConfiguration transformationConfiguration);

    Source getSource();

    List<String> findUnprocessedVersions();

    Class<? extends TransformationConfiguration> getConfigurationClass();

    TransformationConfiguration createTransformationConfiguration(List<String> versionsToProcess);
}
