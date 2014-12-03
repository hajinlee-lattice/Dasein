package com.latticeengines.domain.exposed.skald.model;

import com.latticeengines.domain.exposed.skald.model.FilterDefinition;

public class TargetedModel {
    public TargetedModel(FilterDefinition filter, String model) {
        this.filter = filter;
        this.model = model;
    }
    
    // Serialization constructor.
    public TargetedModel() {
    }
    
    // Target filter that applies to this model.
    public FilterDefinition filter;

    // Model name, not yet bound to a version.
    public String model;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((filter == null) ? 0 : filter.hashCode());
        result = prime * result + ((model == null) ? 0 : model.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TargetedModel other = (TargetedModel) obj;
        if (filter == null) {
            if (other.filter != null)
                return false;
        } else if (!filter.equals(other.filter))
            return false;
        if (model == null) {
            if (other.model != null)
                return false;
        } else if (!model.equals(other.model))
            return false;
        return true;
    }
}
