package com.latticeengines.propdata.engine.transformation.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public interface TransformationService<T extends TransformationConfiguration> {

    TransformationProgress startNewProgress(T transformationConfiguration, String creator);

    TransformationProgress transform(TransformationProgress progress, T transformationConfiguration);

    TransformationProgress finish(TransformationProgress progress);

    String getVersionString(TransformationProgress progress);

    boolean isNewDataAvailable(T transformationConfiguration);

    Source getSource();

    List<String> findUnprocessedBaseVersions();

    List<List<String>> findUnprocessedBaseSourceVersions();

    Class<? extends TransformationConfiguration> getConfigurationClass();

    T createTransformationConfiguration(List<String> baseVersionsToProcess, String targetVersion);

    boolean isManualTriggerred();
}
