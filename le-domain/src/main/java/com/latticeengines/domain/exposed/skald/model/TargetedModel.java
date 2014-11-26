package com.latticeengines.domain.exposed.skald.model;

import com.latticeengines.domain.exposed.skald.model.FilterDefinition;

public class TargetedModel {
    // Target filter that applies to this model.
    FilterDefinition filter;

    // Model name, not yet bound to a version.
    String model;
}
