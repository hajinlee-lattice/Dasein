package com.latticeengines.skald.exposed;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;
import com.latticeengines.domain.exposed.skald.model.ScoreDerivation;

public class SetScoreDerivationRequest {
    public CustomerSpace space;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
