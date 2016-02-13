package com.latticeengines.scoringapi.unused;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.unused.FilterDefinition;
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
