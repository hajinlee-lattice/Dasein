package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.Sampling;

public interface ModelQualitySamplingInterface {

    Sampling createSamplingFromProduction();

    String createSamplingConfig(Sampling samplingConfig);

    List<Sampling> getSamplingConfigs();

    Sampling getSamplingConfigByName(String samplingConfigName);
}
