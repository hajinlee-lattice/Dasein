package com.latticeengines.skald;

import com.latticeengines.domain.exposed.skald.model.DataComposition;
import com.latticeengines.domain.exposed.skald.model.FilterDefinition;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;
import com.latticeengines.domain.exposed.skald.model.ScoreDerivation;
public class CombinationElement {
    public FilterDefinition filter;

    public DataComposition data;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
