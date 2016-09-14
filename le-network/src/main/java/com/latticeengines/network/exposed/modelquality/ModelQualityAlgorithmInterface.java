package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.Algorithm;

public interface ModelQualityAlgorithmInterface {

    List<Algorithm> getAlgorithms();

    Algorithm createAlgorithmFromProduction();

    String createAlgorithm(Algorithm algorithm);

    Algorithm getAlgorithmByName(String algorithmName);

}
