package com.latticeengines.skald.exposed;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.unused.ModelIdentifier;

public class SetScoreDerivationRequest {
    public CustomerSpace space;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
