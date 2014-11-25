package com.latticeengines.skald;

import com.latticeengines.skald.model.DataComposition;
import com.latticeengines.skald.model.FilterDefinition;
import com.latticeengines.skald.model.ModelIdentifier;
import com.latticeengines.skald.model.ScoreDerivation;

public class CombinationElement {
    public FilterDefinition filter;

    public DataComposition data;

    public ModelIdentifier model;

    public ScoreDerivation derivation;
}
