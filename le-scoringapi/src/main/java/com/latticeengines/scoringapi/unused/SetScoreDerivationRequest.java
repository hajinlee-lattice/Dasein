package com.latticeengines.scoringapi.unused;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class SetScoreDerivationRequest {
    public CustomerSpace space;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
