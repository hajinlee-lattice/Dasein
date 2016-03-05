package com.latticeengines.skald;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.unused.FilterDefinition;
import com.latticeengines.domain.exposed.scoringapi.unused.ModelIdentifier;
public class CombinationElement {
    public CombinationElement(FilterDefinition filter, DataComposition data, ModelIdentifier model, ScoreDerivation derivation) {
        this.filter = filter;
        this.data = data;
        this.model = model;
        this.derivation = derivation;
    }
    
    // Serialization Constructor.
    public CombinationElement() {
    }
    
    public FilterDefinition filter;

    public DataComposition data;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
