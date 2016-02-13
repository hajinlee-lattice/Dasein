package com.latticeengines.domain.exposed.scoringapi.unused;

import java.util.ArrayList;

// Ordered list of model names and their target filters. Evaluated against an input record,
// the filter in each element will be run against the record, and the first match will
// determine the model name to use. This will be combined with a model version specified elsewhere.
@SuppressWarnings("serial")
public class ModelCombination extends ArrayList<TargetedModel> {

}
