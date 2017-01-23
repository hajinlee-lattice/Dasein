package com.latticeengines.domain.exposed.pls;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class CompanyProfileAttributeFlags extends AttributeFlags {

    public CompanyProfileAttributeFlags() {
    }

    public CompanyProfileAttributeFlags(boolean hidden, boolean highlighted) {
        this.hidden = hidden;
        this.highlighted = highlighted;
    }

    @JsonProperty
    private boolean hidden;

    @JsonProperty
    private boolean highlighted;

    public boolean isHidden() {
        return hidden;
    }

    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    public boolean isHighlighted() {
        return highlighted;
    }

    public void setHighlighted(boolean highlighted) {
        this.highlighted = highlighted;
    }

    @Override
    public boolean equals(Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
