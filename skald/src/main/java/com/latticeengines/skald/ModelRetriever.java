package com.latticeengines.skald;

import org.springframework.stereotype.Service;

import com.latticeengines.skald.model.PredictiveModel;
import com.latticeengines.skald.model.ScoreDerivation;

@Service
public class ModelRetriever {
    public PredictiveModel getPredictiveModel(CustomerSpaceID spaceID, String modelID) {
        return null;
    }

    public ScoreDerivation getScoreDerivation(CustomerSpaceID spaceID, String modelID) {
        return null;
    }
}
