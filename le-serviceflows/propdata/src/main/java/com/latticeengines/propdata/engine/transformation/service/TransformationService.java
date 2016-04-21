package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public interface TransformationService {

    TransformationProgress startNewProgress(TransformationConfiguration transformationConfiguration, String creator);

    TransformationProgress transform(TransformationProgress progress);

    TransformationProgress finish(TransformationProgress progress);

    String getVersionString(TransformationProgress progress);

    boolean isNewDataAvailable(TransformationConfiguration transformationConfiguration);

    Source getSource();

    String findUnprocessedVersion();

    Class<? extends TransformationConfiguration> getConfigurationClass();

    TransformationConfiguration createTransformationConfiguration(String versionToProcess);
}
