package com.latticeengines.skald;

import com.latticeengines.domain.exposed.skald.model.DataComposition;
import com.latticeengines.domain.exposed.skald.model.FilterDefinition;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;
import com.latticeengines.domain.exposed.skald.model.ScoreDerivation;
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
